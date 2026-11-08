package com.netmax.netbond.engine

// Who should fetch what, decided from a snapshot of the download and nothing
// else - no I/O, no clock of its own - so every rule can be checked against
// exact situations.
//
// A free stream is given one of three kinds of work:
//   primary  A block nobody is fetching. The normal case until the queue runs dry.
//   hedge    A second attempt at a block someone else is fetching too slowly,
//            handed out only once no block is left waiting. Whichever attempt
//            finishes first wins; the others are dropped.
//   split    The back half of what an attempt has left, once no block is waiting
//            and racing wouldn't pay. The attempt stops where the split begins,
//            so nothing is fetched twice.
//
// Ported from Plexo's src/main/download/scheduler.ts (MIT, (c) 2026 Anmol Kapil) - see NOTICE.

enum class WorkKind { PRIMARY, HEDGE, SPLIT }

data class AttemptView(
    val kind: WorkKind,
    val streamId: Int,
    val networkId: String,
    /** When its request was sent, ms. */
    val startedAt: Long,
    /** The reader is paused for writes: the disk, not the connection, sets its pace. */
    val writeWaiting: Boolean = false,
    /** Its part of the block, as offsets into it: from startOffset up to end, exclusive. */
    val startOffset: Long? = null,
    val end: Long? = null,
    /** Bytes it has been sent from startOffset. */
    val networkReceived: Long? = null,
    /** Called off and on its way out: its part is not split. */
    val abortReason: String? = null
)

/** Bytes of a block fetched ahead of its frontier by a split: [start, end). */
data class Segment(val start: Long, val end: Long)

data class SchedulerState(
    val blocks: List<HttpBlockState>,
    val streams: List<HttpStreamState>,
    /** Attempts in flight, by block index. */
    val attempts: Map<Int, List<AttemptView>>,
    /** For a block whose last attempt on some network delivered nothing, that network. */
    val avoid: Map<Int, String>,
    /** How many hedges each block has had so far. */
    val hedgesUsed: Map<Int, Int>,
    /** The disk can't keep up: a hedge would only add writes. */
    val diskBehind: Boolean = false,
    /** By block index, what splits have fetched ahead of its frontier. */
    val segments: Map<Int, List<Segment>> = emptyMap()
)

data class SchedulerPolicy(
    /** A block is only raced once every attempt at it has been going this long. */
    val hedgeAfterMs: Long,
    /** Bounds the duplicate work on a block whose hedges keep failing. */
    val maxHedgesPerBlock: Int,
    /** What a new request costs before its first byte arrives, counted into hedge time. */
    val startupMs: Long,
    /** The least either half of a split may be; null = nothing is split. */
    val minSplitBytes: Long? = null
)

data class Requester(
    val id: Int,
    val networkId: String,
    /** How fast this stream fetched its last block, if it has finished one. */
    val speedBytesPerSec: Double? = null
)

data class Work(
    val kind: WorkKind,
    val block: HttpBlockState,
    /** For a split: its part of the block, [start, end). */
    val start: Long? = null,
    val end: Long? = null
)

private fun lengthOf(block: HttpBlockState): Long? =
    block.rangeEnd?.let { it - block.rangeStart + 1 }

/** Where an attempt starting [from] bytes into [block] stops: where the next
 * part someone is fetching, or has fetched, begins - or the end of the block. */
fun partEnd(
    block: HttpBlockState,
    attempts: List<AttemptView>,
    segments: List<Segment>,
    from: Long
): Long? {
    var end = lengthOf(block) ?: return null
    for (attempt in attempts) {
        val start = attempt.startOffset
        if (start != null && start > from) end = minOf(end, start)
    }
    for (segment in segments) if (segment.start > from) end = minOf(end, segment.start)
    return end
}

/** The first stretch of [block], past its frontier, that nobody is fetching and
 * nobody has: what a split left behind when it stopped early. */
private fun firstGap(
    block: HttpBlockState,
    attempts: List<AttemptView>,
    segments: List<Segment>
): Segment? {
    val length = lengthOf(block) ?: return null
    val taken = mutableListOf(Segment(0, block.bytesDownloaded))
    taken.addAll(segments)
    for (attempt in attempts) {
        val start = attempt.startOffset ?: return null
        taken.add(Segment(start, attempt.end ?: length))
    }
    taken.sortBy { it.start }
    var reach = 0L
    for (part in taken) {
        if (part.start > reach) break
        reach = maxOf(reach, part.end)
    }
    if (reach >= length) return null
    val next = taken.find { it.start > reach }
    return Segment(reach, next?.start ?: length)
}

private fun otherNetworkFree(state: SchedulerState, networkId: String): Boolean =
    state.streams.any { it.interfaceId != networkId && it.status == StreamStatus.PENDING }

/** The first waiting block - except that one whose last attempt on this network
 * delivered nothing is left to another network, for as long as a stream there is
 * free to take it. With no other network free the block is taken anyway. */
private fun nextWaitingBlock(state: SchedulerState, networkId: String): HttpBlockState? {
    val otherFree = otherNetworkFree(state, networkId)
    return state.blocks.find {
        it.status == BlockStatus.PENDING && !(otherFree && state.avoid[it.index] == networkId)
    }
}

/** A stretch of a block in flight that nobody is fetching, past its frontier. */
private fun nextGap(state: SchedulerState, networkId: String): Work? {
    val otherFree = otherNetworkFree(state, networkId)
    for ((index, attempts) in state.attempts) {
        val block = state.blocks.getOrNull(index)
        if (block == null || block.index != index || block.status != BlockStatus.DOWNLOADING) continue
        if (otherFree && state.avoid[index] == networkId) continue
        // Only behind someone fetching from the frontier.
        if (attempts.none { it.kind == WorkKind.PRIMARY }) continue
        val gap = firstGap(block, attempts, state.segments[index] ?: emptyList())
        if (gap != null && gap.start > block.bytesDownloaded) {
            return Work(WorkKind.SPLIT, block, start = gap.start, end = gap.end)
        }
    }
    return null
}

/** The back half of the part with the most bytes left, if both halves are at
 * least minSplitBytes. Only once nothing is waiting, and while the disk keeps up. */
private fun nextSplit(state: SchedulerState, who: Requester, policy: SchedulerPolicy): Work? {
    val min = policy.minSplitBytes ?: return null
    if (state.diskBehind || state.blocks.any { it.status == BlockStatus.PENDING }) return null

    var target: Work? = null
    var most = 2 * min - 1
    for ((index, attempts) in state.attempts) {
        val block = state.blocks.getOrNull(index)
        if (block == null || block.index != index || block.rangeEnd == null ||
            block.status != BlockStatus.DOWNLOADING
        ) continue
        if (state.avoid[index] == who.networkId) continue
        val raced = attempts.any { it.kind == WorkKind.HEDGE }
        for (attempt in attempts) {
            if (attempt.abortReason != null || (raced && attempt.kind != WorkKind.SPLIT)) continue
            val startOffset = attempt.startOffset ?: continue
            val end = attempt.end ?: continue
            val networkReceived = attempt.networkReceived ?: continue
            val reached = startOffset + networkReceived
            val left = end - reached
            if (left > most) {
                most = left
                target = Work(WorkKind.SPLIT, block, start = reached + left / 2, end = end)
            }
        }
    }
    return target
}

/** The block most worth another attempt: the one that will be longest yet, if
 * the requester would clearly beat it. A block is as slow as its fastest attempt. */
private fun nextHedgeTarget(
    state: SchedulerState,
    who: Requester,
    now: Long,
    policy: SchedulerPolicy
): HttpBlockState? {
    if (state.diskBehind || state.blocks.any { it.status == BlockStatus.PENDING }) return null

    fun speedOf(streamId: Int): Double =
        state.streams.find { it.id == streamId }?.speedBytesPerSec ?: 0.0

    var target: HttpBlockState? = null
    var latest = 0.0
    for ((index, attempts) in state.attempts) {
        val block = state.blocks.getOrNull(index)
        if (block == null || block.index != index || block.rangeEnd == null ||
            block.status != BlockStatus.DOWNLOADING
        ) continue
        if (attempts.any { it.streamId == who.id }) continue
        // What is raced is the part fetched from the block's frontier.
        val racing = attempts.filter { it.kind != WorkKind.SPLIT }
        val primary = racing.find { it.kind == WorkKind.PRIMARY } ?: continue
        if ((state.hedgesUsed[index] ?: 0) >= policy.maxHedgesPerBlock) continue
        if (state.avoid[index] == who.networkId) continue
        if (racing.any { now - it.startedAt < policy.hedgeAfterMs }) continue
        if (racing.any { it.writeWaiting }) continue

        val blockLength = block.rangeEnd - block.rangeStart + 1
        val remaining = (primary.end ?: blockLength) - block.bytesDownloaded
        if (remaining <= 0) continue
        // A holder that has gone quiet has no finish time at all.
        val eta = racing.minOf { attempt ->
            val speed = speedOf(attempt.streamId)
            if (speed > 0) (remaining / speed) * 1000 else Double.POSITIVE_INFINITY
        }
        val requesterSpeed = who.speedBytesPerSec
        val worthIt = if (requesterSpeed != null && requesterSpeed > 0) {
            eta >= 2 * (policy.startupMs + (remaining / requesterSpeed) * 1000)
        } else {
            eta >= policy.hedgeAfterMs
        }
        if (!worthIt) continue

        // A stream on another network gets the first go, unless none would take it.
        val holders = racing.map { it.networkId }.toSet()
        val otherFree = state.streams.any {
            it.interfaceId !in holders && it.status == StreamStatus.PENDING &&
                state.avoid[index] != it.interfaceId
        }
        if (who.networkId in holders && otherFree) continue

        if (eta > latest) {
            target = block
            latest = eta
        }
    }
    return target
}

fun pickWork(
    state: SchedulerState,
    who: Requester,
    now: Long,
    policy: SchedulerPolicy
): Work? {
    nextWaitingBlock(state, who.networkId)?.let { return Work(WorkKind.PRIMARY, it) }
    nextGap(state, who.networkId)?.let { return it }
    nextHedgeTarget(state, who, now, policy)?.let { return Work(WorkKind.HEDGE, it) }
    return nextSplit(state, who, policy)
}
