package com.netmax.netbond.dl

import com.netmax.netbond.engine.Action
import com.netmax.netbond.engine.AttemptView
import com.netmax.netbond.engine.BlockStatus
import com.netmax.netbond.engine.ConcurrencyController
import com.netmax.netbond.engine.HttpBlockState
import com.netmax.netbond.engine.HttpStreamState
import com.netmax.netbond.engine.MAX_STREAMS_PER_NETWORK
import com.netmax.netbond.engine.NetworkSnapshot
import com.netmax.netbond.engine.Requester
import com.netmax.netbond.engine.SchedulerPolicy
import com.netmax.netbond.engine.SchedulerState
import com.netmax.netbond.engine.Segment
import com.netmax.netbond.engine.Snapshot
import com.netmax.netbond.engine.StreamStatus
import com.netmax.netbond.engine.Work
import com.netmax.netbond.engine.WorkKind
import com.netmax.netbond.engine.advanceBlock
import com.netmax.netbond.engine.partEnd
import com.netmax.netbond.engine.pickWork
import com.netmax.netbond.engine.planBlocks
import com.netmax.netbond.engine.planDownload
import com.netmax.netbond.engine.PlanRequest
import com.netmax.netbond.engine.startingStreams
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONArray
import org.json.JSONObject
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
 * machinery around it: coroutine streams, bound HTTP range requests,
 * RandomAccessFile block writes, progress accounting, pause/resume.
 *
 * Transport-agnostic (NetTransport), so JVM unit tests run the whole engine
 * against a local server. The Android side supplies BoundNetwork instances.
 *
 * Frontier rule (as in Plexo): a block's bytesDownloaded is its frontier - the
 * bytes from its first one that are secured. Only attempts fetching FROM the
 * frontier (primary/hedge) advance it. Split bytes sit ahead of it and count
 * once the frontier reaches them (see [reconcile]).
 */
class DownloadEngine(
    private val scope: CoroutineScope,
    private val url: URL,
    private val destination: File,
    initialNetworks: List<NetTransport>,
    private val onProgress: (downloaded: Long, total: Long, perNetwork: Map<String, Long>) -> Unit,
    private val onComplete: (file: File) -> Unit = {},
    private val onFailed: (reason: String) -> Unit = {}
) {
    /** One in-flight request. Positions are offsets INTO the block. */
    private class Attempt(
        val kind: WorkKind,
        val streamId: Int,
        val networkId: String,
        val startedAt: Long,
        val startOffset: Long,
        @Volatile var end: Long?, // exclusive; null = to end of block
        @Volatile var received: Long = 0,
        @Volatile var aborted: String? = null
    )

    /** Bytes a split fetched ahead of its block's frontier, with their network. */
    private data class FetchedSegment(val start: Long, val end: Long, val networkId: String)

    private val stateLock = Mutex()
    private val blocks = mutableListOf<HttpBlockState>()
    private var totalBytes: Long = 0
    private var splittable = true

    private val attempts = mutableMapOf<Int, MutableList<Attempt>>()
    private val segments = mutableMapOf<Int, MutableList<FetchedSegment>>()
    private val avoid = mutableMapOf<Int, String>()
    private val hedgesUsed = mutableMapOf<Int, Int>()
    private val retryNotBefore = mutableMapOf<Int, Long>()
    private val retryCount = mutableMapOf<Int, Int>()

    private val streamStates = ConcurrentHashMap<Int, HttpStreamState>()
    private val streamJobs = ConcurrentHashMap<Int, Job>()
    private val nextStreamId = AtomicInteger(1)
    private val retiring = ConcurrentHashMap.newKeySet<Int>()

    private val concurrency = ConcurrencyController(MAX_STREAMS_PER_NETWORK)
    private val refusedSinceTick = mutableMapOf<String, Int>()

    @Volatile private var paused = false
    @Volatile private var stopped = false
    private var supervisor: Job? = null
    private var lastSave = 0L

    /** Networks in use; grows when a new one (e.g. requested cellular) joins. */
    private val networks = java.util.concurrent.CopyOnWriteArrayList(initialNetworks)

    /** A network that joined mid-download gets its starting streams at once. */
    fun addNetwork(network: NetTransport) {
        if (stopped || networks.any { it.id == network.id }) return
        networks.add(network)
        if (blocks.isNotEmpty()) {
            repeat(startingStreams(waitingCount(), networks.size)) { launchStream(network) }
        }
    }

    private val policy = SchedulerPolicy(
        hedgeAfterMs = 4_000,
        maxHedgesPerBlock = 3,
        startupMs = 1_500,
        minSplitBytes = 1 * 1024 * 1024
    )

    private companion object {
        const val MAX_BLOCK_RETRIES = 8
    }

    /** Probe the server, plan (or restore) blocks, then run to completion. */
    fun start() {
        supervisor = scope.launch(Dispatchers.IO) {
            try {
                probe()
                require(networks.isNotEmpty()) { "No internet networks available" }
                preallocate()
                for (network in networks) {
                    repeat(startingStreams(waitingCount(), networks.size)) {
                        launchStream(network)
                    }
                }
                while (isActive && !stopped) {
                    if (!paused) {
                        tickConcurrency()
                        reportProgress()
                        maybeSave()
                        if (finished()) {
                            sidecar.delete()
                            onComplete(destination)
                            return@launch
                        }
                    }
                    delay(400)
                }
            } catch (e: Exception) {
                if (!stopped) onFailed(e.message ?: e.javaClass.simpleName)
            }
        }
    }

    /** Pause: streams stop at the next chunk, progress is saved. */
    suspend fun pause() {
        paused = true
        delay(600) // let read loops notice
        stateLock.withLock {
            for ((index, block) in blocks.withIndex()) {
                if (block.status == BlockStatus.DOWNLOADING && attempts[index].isNullOrEmpty()) {
                    block.status = BlockStatus.PENDING
                }
            }
        }
        save()
    }

    /** Resume after [pause]: free streams ask for work again. */
    fun resume() {
        paused = false
    }

    /** Stop for good; progress stays saved for a later [start]. */
    fun stop() {
        stopped = true
        supervisor?.cancel()
        for (job in streamJobs.values) job.cancel()
        runCatching { save() }
    }

    // ---- probe and plan ----------------------------------------------------

    private fun probe() {
        val network = networks.first()
        var length = -1L
        var ranges = false
        runCatching {
            val head = network.openConnection(url).apply {
                requestMethod = "HEAD"
                connectTimeout = 15_000
                readTimeout = 15_000
                instanceFollowRedirects = true
            }
            length = head.contentLengthLong
            ranges = head.getHeaderField("Accept-Ranges") == "bytes"
            head.disconnect()
        }
        if (length <= 0) {
            // Some servers answer HEAD without a length: a 1-byte GET's
            // Content-Range carries the total.
            runCatching {
                val conn = network.openConnection(url).apply {
                    connectTimeout = 15_000
                    readTimeout = 15_000
                    instanceFollowRedirects = true
                    setRequestProperty("Range", "bytes=0-0")
                }
                if (conn.responseCode == 206) {
                    // Content-Range: bytes 0-0/TOTAL
                    val cr = conn.getHeaderField("Content-Range")
                    length = cr?.substringAfter('/', "-1")?.toLongOrNull() ?: -1
                    ranges = true
                }
                conn.inputStream.close()
                conn.disconnect()
            }
        }
        totalBytes = length
        splittable = ranges
        val plan = planDownload(
            PlanRequest(totalBytes = length, splittable = ranges, networkCount = networks.size)
        )
        val planned = planBlocks(length, plan.blockSizeBytes)
        if (!restore(planned)) {
            blocks.addAll(planned)
        }
    }

    private fun preallocate() {
        if (totalBytes > 0 && destination.length() != totalBytes) {
            RandomAccessFile(destination, "rw").use { it.setLength(totalBytes) }
        }
    }

    // ---- saved progress ------------------------------------------------------

    private val sidecar: File get() = File(destination.parentFile, destination.name + ".netbond.json")

    private fun save() {
        if (blocks.isEmpty()) return
        val json = JSONObject()
        json.put("url", url.toString())
        json.put("totalBytes", totalBytes)
        json.put("splittable", splittable)
        val arr = JSONArray()
        for (block in blocks) {
            val b = JSONObject()
            b.put("index", block.index)
            b.put("frontier", block.bytesDownloaded)
            val byNet = JSONObject()
            for ((id, bytes) in block.bytesByInterface) byNet.put(id, bytes)
            b.put("byNet", byNet)
            arr.put(b)
        }
        json.put("blocks", arr)
        sidecar.writeText(json.toString())
    }

    private fun maybeSave() {
        val now = System.currentTimeMillis()
        if (now - lastSave > 2_000) {
            lastSave = now
            runCatching { save() }
        }
    }

    /** Restores frontiers from the sidecar into [planned]; true when restored. */
    private fun restore(planned: List<HttpBlockState>): Boolean {
        if (!sidecar.exists() || !destination.exists()) return false
        val json = runCatching { JSONObject(sidecar.readText()) }.getOrNull() ?: return false
        if (json.optString("url") != url.toString()) return false
        if (json.optLong("totalBytes", -2) != totalBytes) return false
        val arr = json.optJSONArray("blocks") ?: return false
        if (arr.length() != planned.size) return false
        for (i in 0 until arr.length()) {
            val b = arr.getJSONObject(i)
            val block = planned[i]
            val frontier = b.optLong("frontier", 0)
            block.bytesDownloaded = frontier
            val byNet = b.optJSONObject("byNet")
            byNet?.keys()?.forEach { id -> block.bytesByInterface[id] = byNet.getLong(id) }
            val length = block.rangeEnd?.let { it - block.rangeStart + 1 }
            block.status = if (length != null && frontier >= length) {
                BlockStatus.DONE
            } else {
                BlockStatus.PENDING
            }
        }
        blocks.addAll(planned)
        return true
    }

    // ---- streams ---------------------------------------------------------------

    private fun waitingCount(): Int = blocks.count { it.status == BlockStatus.PENDING }

    private fun finished(): Boolean =
        blocks.isNotEmpty() && blocks.all { it.status == BlockStatus.DONE }

    private fun launchStream(network: NetTransport) {
        val id = nextStreamId.getAndIncrement()
        streamStates[id] = HttpStreamState(id, network.id, StreamStatus.PENDING, 0.0)
        streamJobs[id] = scope.launch(Dispatchers.IO) { streamLoop(id, network) }
    }

    private suspend fun streamLoop(streamId: Int, network: NetTransport) {
        while (scope.isActive && !stopped) {
            if (paused || streamId in retiring) break
            val work = stateLock.withLock {
                streamStates[streamId] = streamStates[streamId]!!.copy(status = StreamStatus.PENDING)
                val now = System.currentTimeMillis()
                pickWork(snapshot(now), requester(streamId), now, policy)?.also { work ->
                    claim(streamId, network, work)
                }
            }
            if (work == null) {
                if (finished() || (waitingCount() == 0 && noAttemptsRunning())) break
                delay(200)
                continue
            }
            runAttempt(streamId, network, work)
        }
        streamStates.remove(streamId)
        streamJobs.remove(streamId)
    }

    private fun requester(streamId: Int): Requester {
        val s = streamStates[streamId]!!
        return Requester(s.id, s.interfaceId, s.speedBytesPerSec.takeIf { it > 0 })
    }

    /** Registers the attempt for a picked work item; for a split, cuts the
     * attempt it took the back half from so nothing is fetched twice. */
    private fun claim(streamId: Int, network: NetTransport, work: Work) {
        val block = work.block
        if (block.status == BlockStatus.PENDING) block.status = BlockStatus.DOWNLOADING
        if (work.kind == WorkKind.HEDGE) hedgesUsed.merge(block.index, 1, Int::plus)
        val list = attempts.getOrPut(block.index) { mutableListOf() }
        if (work.kind == WorkKind.SPLIT && work.start != null) {
            // The attempt fetching across the split's start stops there.
            for (a in list) {
                val aEnd = a.end
                if (a.aborted == null && aEnd != null &&
                    a.startOffset < work.start && aEnd > work.start
                ) {
                    a.end = work.start
                }
            }
        }
        val startOffset = work.start ?: block.bytesDownloaded
        val end = work.end ?: partEnd(
            block,
            list.map { it.toView() },
            (segments[block.index] ?: emptyList()).map { Segment(it.start, it.end) },
            startOffset
        )
        list.add(
            Attempt(
                kind = work.kind,
                streamId = streamId,
                networkId = network.id,
                startedAt = System.currentTimeMillis(),
                startOffset = startOffset,
                end = end
            )
        )
        streamStates[streamId] = streamStates[streamId]!!.copy(status = StreamStatus.DOWNLOADING)
    }

    private fun noAttemptsRunning(): Boolean = attempts.values.all { it.isEmpty() }

    private fun snapshot(now: Long): SchedulerState {
        // Blocks waiting out a retry backoff are presented as in-flight so the
        // scheduler leaves them alone until their time comes.
        val presented = blocks.map { block ->
            val held = (retryNotBefore[block.index] ?: 0) > now &&
                attempts[block.index].isNullOrEmpty()
            if (held && block.status == BlockStatus.PENDING) {
                block.copy(status = BlockStatus.DOWNLOADING, bytesByInterface = mutableMapOf())
            } else block
        }
        return SchedulerState(
            blocks = presented,
            streams = streamStates.values.toList(),
            attempts = attempts.mapValues { it.value.map { a -> a.toView() } },
            avoid = avoid.toMap(),
            hedgesUsed = hedgesUsed.toMap(),
            diskBehind = false,
            segments = segments.mapValues { segs -> segs.value.map { Segment(it.start, it.end) } }
        )
    }

    private fun Attempt.toView() = AttemptView(
        kind = kind,
        streamId = streamId,
        networkId = networkId,
        startedAt = startedAt,
        writeWaiting = false,
        startOffset = startOffset,
        end = end,
        networkReceived = received,
        abortReason = aborted
    )

    // ---- fetching ----------------------------------------------------------------

    private suspend fun runAttempt(streamId: Int, network: NetTransport, work: Work) {
        val block = work.block
        val attempt = attempts[block.index]!!.last { it.streamId == streamId }
        val blockLength = block.rangeEnd?.let { it - block.rangeStart + 1 }
        var speed = 0.0
        var error: Exception? = null
        try {
            val rangeStart = block.rangeStart + attempt.startOffset
            val rangeEnd = attempt.end.let { end ->
                when {
                    end != null -> block.rangeStart + end - 1
                    blockLength != null -> block.rangeStart + blockLength - 1
                    else -> null
                }
            }
            val opened = ChunkFetcher.open(url, network, rangeStart, rangeEnd)
            RandomAccessFile(destination, "rw").use { out ->
                opened.body.use { input ->
                    out.seek(rangeStart)
                    val buffer = ByteArray(64 * 1024)
                    while (true) {
                        if (attempt.aborted != null || paused || stopped) break
                        val limit = attempt.end ?: blockLength ?: Long.MAX_VALUE
                        val position = attempt.startOffset + attempt.received
                        if (position >= limit) break
                        val read = input.read(
                            buffer, 0,
                            minOf(buffer.size.toLong(), limit - position).toInt()
                        )
                        if (read < 0) break
                        out.write(buffer, 0, read)
                        attempt.received += read
                        if (attempt.kind != WorkKind.SPLIT) {
                            stateLock.withLock {
                                advanceBlock(block, network.id, position + read)
                                reconcile(block)
                                finishIfDone(block)
                            }
                        }
                    }
                }
            }
            speed = attempt.received * 1000.0 /
                (System.currentTimeMillis() - attempt.startedAt).coerceAtLeast(1)
        } catch (e: Exception) {
            error = e
        }
        stateLock.withLock {
            attempts[block.index]?.remove(attempt)
            if (paused || stopped) {
                if (attempts[block.index].isNullOrEmpty() &&
                    block.status == BlockStatus.DOWNLOADING
                ) {
                    block.status = BlockStatus.PENDING
                }
            } else if (error != null) {
                onAttemptError(block, attempt, network, error)
            } else {
                if (attempt.kind == WorkKind.SPLIT && attempt.received > 0) {
                    segments.getOrPut(block.index) { mutableListOf() }.add(
                        FetchedSegment(
                            attempt.startOffset,
                            attempt.startOffset + attempt.received,
                            network.id
                        )
                    )
                }
                if (attempt.received == 0L && attempt.aborted == null) {
                    avoid[block.index] = network.id
                }
                retryCount.remove(block.index)
            }
            reconcile(block)
            finishIfDone(block)
        }
        streamStates[streamId]?.let { st ->
            streamStates[streamId] = st.copy(status = StreamStatus.PENDING, speedBytesPerSec = speed)
        }
    }

    private fun onAttemptError(
        block: HttpBlockState,
        attempt: Attempt,
        network: NetTransport,
        e: Exception
    ) {
        if (attempt.kind == WorkKind.SPLIT && attempt.received > 0) {
            segments.getOrPut(block.index) { mutableListOf() }.add(
                FetchedSegment(attempt.startOffset, attempt.startOffset + attempt.received, network.id)
            )
        }
        if (attempt.received == 0L) avoid[block.index] = network.id
        if (e is ChunkFetcher.HttpStatusError && e.transient) {
            // A server limiting connections says so by refusing while serving
            // others: the concurrency controller sees the refusal.
            refusedSinceTick.merge(network.id, 1, Int::plus)
        }
        val count = (retryCount[block.index] ?: 0) + 1
        retryCount[block.index] = count
        if (count > MAX_BLOCK_RETRIES) {
            // A block that keeps failing (bad link, server gone) fails the
            // download rather than retrying forever.
            block.status = BlockStatus.FAILED
            stopped = true
            onFailed("Block ${block.index} failed after $count tries: ${e.message}")
            return
        }
        val headerWait = (e as? ChunkFetcher.HttpStatusError)?.retryAfterMs ?: 0
        retryNotBefore[block.index] = System.currentTimeMillis() +
            maxOf(headerWait, minOf(1_000L shl minOf(count, 5), 30_000L))
        // With nobody left fetching it, the block goes back to the queue.
        val remaining = attempts[block.index]?.count { it.aborted == null } ?: 0
        if (remaining == 0 && block.status == BlockStatus.DOWNLOADING) {
            block.status = BlockStatus.PENDING
        }
    }

    /** The frontier jumps over bytes splits already fetched ahead of it. */
    private fun reconcile(block: HttpBlockState) {
        val segs = segments[block.index] ?: return
        var moved = true
        while (moved) {
            moved = false
            val covering = segs.find {
                it.start <= block.bytesDownloaded && it.end > block.bytesDownloaded
            }
            if (covering != null) {
                advanceBlock(block, covering.networkId, covering.end)
                segs.removeAll { it.end <= block.bytesDownloaded }
                moved = true
            }
        }
    }

    private fun finishIfDone(block: HttpBlockState) {
        val length = block.rangeEnd?.let { it - block.rangeStart + 1 }
        val done = if (length != null) {
            block.bytesDownloaded >= length
        } else {
            // Unknown length: done once an attempt has hit EOF and nobody is
            // left fetching (EOF ends the read loop with position at the end).
            block.bytesDownloaded > 0 &&
                attempts[block.index]?.none { it.aborted == null } != false
        }
        if (done && block.status != BlockStatus.DONE) {
            block.status = BlockStatus.DONE
            attempts[block.index]?.forEach { it.aborted = "done" }
        }
    }

    // ---- concurrency + progress ----------------------------------------------

    private fun tickConcurrency() {
        val now = System.currentTimeMillis()
        val byNetwork = streamStates.values.groupBy { it.interfaceId }
        val snap = Snapshot(
            now = now,
            networks = byNetwork.map { (id, streams) ->
                NetworkSnapshot(
                    id = id,
                    streams = streams.count { it.status != StreamStatus.RETIRING },
                    answered = streams.count { it.speedBytesPerSec > 0 },
                    refused = refusedSinceTick[id] ?: 0,
                    served = streams.count { it.status == StreamStatus.DOWNLOADING }
                )
            },
            spareWork = waitingCount(),
            disk = null // flash storage on phones: no disk-throttling needed
        )
        refusedSinceTick.clear()
        for (action in concurrency.tick(snap)) {
            when (action) {
                is Action.Add -> {
                    val network = networks.find { it.id == action.networkId } ?: continue
                    repeat(action.count) { launchStream(network) }
                }
                is Action.Retire -> {
                    // Streams retire cooperatively after their current chunk.
                    streamStates.values
                        .filter { it.interfaceId == action.networkId }
                        .sortedByDescending { it.id }
                        .take(action.count)
                        .forEach { retiring.add(it.id) }
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
