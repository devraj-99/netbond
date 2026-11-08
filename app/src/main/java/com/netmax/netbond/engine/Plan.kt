package com.netmax.netbond.engine

// How a download is cut into blocks, and how many streams a network starts on.
// Pure: every rule can be checked against exact situations.
// Ported from Plexo's src/main/download/plan.ts (MIT, (c) 2026 Anmol Kapil) - see NOTICE.

const val MIB = 1024L * 1024L

/** The largest block. A racing second attempt re-fetches what is left of a block
 * rather than splitting it, so the last block's size is how long the slowest
 * connection can hold the download up. */
const val DEFAULT_MAX_BLOCK_BYTES = 8 * MIB

/** Below about a megabyte the round trips cost more than splitting saves. */
const val MIN_BLOCK_BYTES = MIB

/** Streams each network starts with (IDM, XDM: 8). Doubles once all receive. */
const val START_STREAMS_PER_NETWORK = 8

/** The most streams a network can grow to (IDM's limit). */
const val MAX_STREAMS_PER_NETWORK = 32

/** Blocks planned per stream a network could grow to. */
private const val BLOCKS_PER_STREAM = 2

data class DownloadPlan(val blockSizeBytes: Long, val blockCount: Int)

data class PlanRequest(
    /** 0 when unknown. */
    val totalBytes: Long,
    /** false when the server can't serve byte ranges. */
    val splittable: Boolean,
    /** The networks it starts on. More can join later; this only sizes blocks. */
    val networkCount: Int,
    val maxBlockBytes: Long = DEFAULT_MAX_BLOCK_BYTES
)

/** Lays [items] out one from each group in turn (a, b, a, b, ...), keeping each
 * group's own order. Groups are visited in order of first appearance. */
fun <T> interleave(items: List<T>, groupOf: (T) -> Any?): List<T> {
    val lanes = LinkedHashMap<Any?, MutableList<T>>()
    for (item in items) lanes.getOrPut(groupOf(item)) { mutableListOf() }.add(item)
    val out = ArrayList<T>(items.size)
    var round = 0
    while (out.size < items.size) {
        for (lane in lanes.values) if (round < lane.size) out.add(lane[round])
        round++
    }
    return out
}

fun planDownload(request: PlanRequest): DownloadPlan {
    val networkCount = maxOf(1, request.networkCount)

    // One request has to carry the whole file.
    if (!request.splittable || request.totalBytes <= 0) {
        return DownloadPlan(blockSizeBytes = maxOf(request.totalBytes, 0), blockCount = 1)
    }

    val targetBlocks = networkCount.toLong() * MAX_STREAMS_PER_NETWORK * BLOCKS_PER_STREAM
    val blockSizeBytes = ceilDiv(request.totalBytes, targetBlocks)
        .coerceIn(minOf(MIN_BLOCK_BYTES, request.maxBlockBytes), request.maxBlockBytes)
    return DownloadPlan(
        blockSizeBytes = blockSizeBytes,
        blockCount = ceilDiv(request.totalBytes, blockSizeBytes).toInt()
    )
}

/** Streams to start on each network joining a download with [waiting] blocks
 * nobody has taken. A stream with no block to claim would only sit idle, so near
 * the end, or on a small file, fewer - but never none, so a network that joins
 * can still race a slow block (see Scheduler). */
fun startingStreams(
    waiting: Int,
    networkCount: Int,
    requested: Int = START_STREAMS_PER_NETWORK
): Int {
    val perNetwork = ceilDiv(waiting.toLong(), maxOf(1, networkCount).toLong()).toInt()
    return maxOf(1, minOf(requested.coerceIn(1, MAX_STREAMS_PER_NETWORK), perNetwork))
}

/** The blocks a file of [totalBytes] is cut into, none of them started. An
 * unknown size is one block, open-ended to the end of the file. */
fun planBlocks(totalBytes: Long, blockSizeBytes: Long): List<HttpBlockState> {
    fun fresh(index: Int, rangeStart: Long, rangeEnd: Long?) = HttpBlockState(
        index = index,
        rangeStart = rangeStart,
        rangeEnd = rangeEnd
    )
    if (totalBytes <= 0) return listOf(fresh(0, 0, null))
    val count = ceilDiv(totalBytes, blockSizeBytes).toInt()
    return (0 until count).map { index ->
        val rangeStart = index * blockSizeBytes
        fresh(index, rangeStart, minOf(rangeStart + blockSizeBytes, totalBytes) - 1)
    }
}

internal fun ceilDiv(a: Long, b: Long): Long = if (b <= 0) 0 else (a + b - 1) / b
