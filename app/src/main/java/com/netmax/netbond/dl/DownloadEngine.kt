package com.netmax.netbond.dl

import com.netmax.netbond.engine.Action
import com.netmax.netbond.engine.AttemptView
import com.netmax.netbond.engine.BlockStatus
import com.netmax.netbond.engine.ConcurrencyController
import com.netmax.netbond.engine.DiskWatch
import com.netmax.netbond.engine.HttpStreamState
import com.netmax.netbond.engine.MAX_STREAMS_PER_NETWORK
import com.netmax.netbond.engine.NetworkSnapshot
import com.netmax.netbond.engine.Requester
import com.netmax.netbond.engine.SchedulerPolicy
import com.netmax.netbond.engine.SchedulerState
import com.netmax.netbond.engine.Snapshot
import com.netmax.netbond.engine.StreamStatus
import com.netmax.netbond.engine.Work
import com.netmax.netbond.engine.WorkKind
import com.netmax.netbond.engine.advanceBlock
import com.netmax.netbond.engine.pickWork
import com.netmax.netbond.engine.planBlocks
import com.netmax.netbond.engine.planDownload
import com.netmax.netbond.engine.PlanRequest
import com.netmax.netbond.engine.startingStreams
import com.netmax.netbond.net.BoundNetwork
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.File
import java.io.RandomAccessFile
import java.net.URL
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Drives one file download across several bound networks at once.
 *
 * The deciding logic (block plan, who fetches what, how many streams each
 * network runs) is the Plexo port in the engine package. This class is the
 * Android-side machinery around it: coroutine streams, bound HTTP range
 * requests, RandomAccessFile block writes, progress accounting.
 *
 * SCAFFOLD STATE: the core loop is wired end-to-end but not yet device-tested.
 * Known gaps before v1: pause/resume with saved progress, hedges/splits are
 * scheduled but split-stop signalling to in-flight streams is TODO, speed
 * limits, and retry-with-backoff polish.
 */
class DownloadEngine(
    private val scope: CoroutineScope,
    private val url: URL,
    private val destination: File,
    private val networks: List<BoundNetwork>,
    private val onProgress: (downloaded: Long, total: Long, perNetwork: Map<String, Long>) -> Unit
) {
    private val stateLock = Mutex()
    private val blocks = run {
        // The real total comes from a probe; the engine is started after start() probes.
        mutableListOf<com.netmax.netbond.engine.HttpBlockState>()
    }
    private var totalBytes: Long = 0
    private val attempts = ConcurrentHashMap<Int, MutableList<AttemptView>>()
    private val avoid = ConcurrentHashMap<Int, String>()
    private val hedgesUsed = ConcurrentHashMap<Int, Int>()
    private val streamStates = ConcurrentHashMap<Int, HttpStreamState>()
    private val nextStreamId = AtomicInteger(1)
    private val concurrency = ConcurrencyController(MAX_STREAMS_PER_NETWORK)
    private val diskWatch = DiskWatch()
    private var job: Job? = null

    private val policy = SchedulerPolicy(
        hedgeAfterMs = 4_000,
        maxHedgesPerBlock = 3,
        startupMs = 1_500,
        minSplitBytes = 1 * 1024 * 1024
    )

    /** Probe the file (size + range support), plan blocks, then run. */
    fun start() {
        job = scope.launch(Dispatchers.IO) {
            probe()
            require(networks.isNotEmpty()) { "No internet networks available" }
            for (network in networks) {
                repeat(startingStreams(waitingCount(), networks.size)) {
                    launchStream(network)
                }
            }
            // Concurrency + progress loop.
            while (isActive && !finished()) {
                tickConcurrency()
                reportProgress()
                delay(500)
            }
        }
    }

    fun stop() {
        job?.cancel()
    }

    private suspend fun probe() {
        // HEAD first; some servers answer HEAD without a length, so fall back to
        // a 1-byte GET.
        val network = networks.first()
        val head = network.openConnection(url).apply {
            requestMethod = "HEAD"
            connectTimeout = 15_000
            readTimeout = 15_000
            instanceFollowRedirects = true
        }
        val length = head.contentLengthLong
        val acceptsRanges = head.getHeaderField("Accept-Ranges") == "bytes"
        head.disconnect()
        totalBytes = length
        val plan = planDownload(
            PlanRequest(
                totalBytes = length,
                splittable = acceptsRanges,
                networkCount = networks.size
            )
        )
        blocks.addAll(planBlocks(length, plan.blockSizeBytes))
    }

    private fun waitingCount(): Int = blocks.count { it.status == BlockStatus.PENDING }

    private fun finished(): Boolean =
        blocks.isNotEmpty() && blocks.all { it.status == BlockStatus.DONE }

    private fun launchStream(network: BoundNetwork) {
        val id = nextStreamId.getAndIncrement()
        streamStates[id] = HttpStreamState(id, network.id, StreamStatus.PENDING, 0.0)
        scope.launch(Dispatchers.IO) {
            streamLoop(id, network)
        }
    }

    private suspend fun streamLoop(streamId: Int, network: BoundNetwork) {
        while (scope.isActive) {
            val work = stateLock.withLock {
                streamStates[streamId] = streamStates[streamId]!!.copy(status = StreamStatus.PENDING)
                val now = System.currentTimeMillis()
                pickWork(snapshot(now), requester(streamId), now, policy)
            }
            if (work == null) {
                if (finished()) return
                delay(200)
                continue
            }
            runAttempt(streamId, network, work)
        }
    }

    private fun requester(streamId: Int): Requester {
        val s = streamStates[streamId]!!
        return Requester(s.id, s.interfaceId, s.speedBytesPerSec.takeIf { it > 0 })
    }

    private fun snapshot(now: Long) = SchedulerState(
        blocks = blocks,
        streams = streamStates.values.toList(),
        attempts = attempts.mapValues { it.value.toList() },
        avoid = avoid.toMap(),
        hedgesUsed = hedgesUsed.toMap(),
        diskBehind = diskWatch.reading == com.netmax.netbond.engine.DiskReading.BEHIND
    )

    private suspend fun runAttempt(streamId: Int, network: BoundNetwork, work: Work) {
        val block = work.block
        stateLock.withLock {
            if (block.status == BlockStatus.PENDING) block.status = BlockStatus.DOWNLOADING
            if (work.kind == WorkKind.HEDGE) hedgesUsed.merge(block.index, 1, Int::plus)
            streamStates[streamId] = streamStates[streamId]!!.copy(status = StreamStatus.DOWNLOADING)
        }
        val startOffset = work.start ?: (block.rangeStart + block.bytesDownloaded)
        val endInclusive = work.end?.let { it - 1 } ?: block.rangeEnd
        val attempt = AttemptView(
            kind = work.kind,
            streamId = streamId,
            networkId = network.id,
            startedAt = System.currentTimeMillis(),
            startOffset = startOffset - block.rangeStart,
            end = endInclusive?.let { it - block.rangeStart + 1 },
            networkReceived = 0
        )
        attempts.getOrPut(block.index) { mutableListOf() }.add(attempt)

        var received = 0L
        var speedStart = System.currentTimeMillis()
        try {
            RandomAccessFile(destination, "rw").use { out ->
                val opened = ChunkFetcher.open(url, network, startOffset, endInclusive)
                opened.body.use { input ->
                    val buffer = ByteArray(64 * 1024)
                    out.seek(startOffset)
                    while (true) {
                        val read = input.read(buffer)
                        if (read < 0) break
                        out.write(buffer, 0, read)
                        received += read
                        diskWatch.landed(System.currentTimeMillis())
                        stateLock.withLock {
                            advanceBlock(block, network.id, block.bytesDownloaded + read)
                        }
                    }
                }
            }
            stateLock.withLock {
                if (block.rangeEnd == null ||
                    block.rangeStart + block.bytesDownloaded > block.rangeEnd!!
                ) {
                    block.status = BlockStatus.DONE
                }
            }
        } catch (e: Exception) {
            stateLock.withLock {
                if (received == 0L) avoid[block.index] = network.id
                if (block.status != BlockStatus.DONE) block.status = BlockStatus.PENDING
            }
        } finally {
            attempts[block.index]?.removeIf { it.streamId == streamId && it.startOffset == attempt.startOffset }
            val elapsed = (System.currentTimeMillis() - speedStart).coerceAtLeast(1)
            streamStates[streamId] = streamStates[streamId]!!.copy(
                status = StreamStatus.PENDING,
                speedBytesPerSec = if (received > 0) received * 1000.0 / elapsed else 0.0
            )
        }
    }

    private fun tickConcurrency() {
        val now = System.currentTimeMillis()
        val byNetwork = streamStates.values.groupBy { it.interfaceId }
        val snap = Snapshot(
            now = now,
            networks = byNetwork.map { (id, streams) ->
                NetworkSnapshot(
                    id = id,
                    streams = streams.count { it.status != StreamStatus.RETIRING },
                    answered = streams.count { it.speedBytesPerSec > 0 || it.status == StreamStatus.DOWNLOADING },
                    refused = 0, // tracked per refusal handling in runAttempt; TODO wire through
                    served = streams.count { it.status == StreamStatus.DOWNLOADING }
                )
            },
            spareWork = waitingCount(),
            disk = null // DiskWatch feeds in once write-wait tracking lands; TODO
        )
        for (action in concurrency.tick(snap)) {
            when (action) {
                is Action.Add -> {
                    val network = networks.find { it.id == action.networkId } ?: continue
                    repeat(action.count) { launchStream(network) }
                }
                is Action.Retire -> {
                    // Streams retire cooperatively at block end; mark them here. TODO
                }
            }
        }
    }

    private fun reportProgress() {
        val downloaded = blocks.sumOf { it.bytesDownloaded }
        val perNetwork = mutableMapOf<String, Long>()
        for (block in blocks) {
            for ((id, bytes) in block.bytesByInterface) {
                perNetwork[id] = (perNetwork[id] ?: 0) + bytes
            }
        }
        onProgress(downloaded, totalBytes, perNetwork)
    }
}
