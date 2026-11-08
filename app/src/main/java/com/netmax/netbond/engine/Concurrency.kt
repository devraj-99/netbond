package com.netmax.netbond.engine

// How many streams each network runs. Deliberately simple, after Gopeed and
// aria2: no timers and no speed comparisons, so noise on a flaky network can't
// move it.
//
// - A network starts with START_STREAMS_PER_NETWORK, or the user's pick.
// - Once every one of its streams has received data, it doubles, up to the limit.
// - A server that refuses some of a network's streams (503/429/403/silence) while
//   serving others wants fewer connections: the refused streams close and the
//   network stays at what's left; after RECOVER_MS without another refusal it may
//   have one more, and so on back up.
// - A disk that can't keep up cuts every network to half the most any runs; each
//   DISK_RECOVER_MS it keeps up at the cap, the cap doubles.
// - Downloads running at once share a network's limit via [share].
//
// Ported from Plexo's src/main/download/concurrency.ts (MIT, (c) 2026 Anmol Kapil) - see NOTICE.

/** How long a lowered limit holds before it may rise by one. */
const val RECOVER_MS = 60_000L
/** How long the disk must stay behind before streams are cut. */
const val DISK_PATIENCE_MS = 500L
/** How long the disk must keep up before the cap it set rises. */
const val DISK_RECOVER_MS = 10_000L
/** No write landed for this long: the disk has stopped rather than slowed down. */
const val DISK_STALLED_MS = 500L

data class NetworkSnapshot(
    val id: String,
    /** Its streams, not counting any retiring. */
    val streams: Int,
    /** Its streams that have received data. */
    val answered: Int,
    /** Its streams the server refused since the last snapshot. */
    val refused: Int,
    /** Its streams that received data since the last snapshot. */
    val served: Int
)

enum class DiskReading { BEHIND, KEEPING_UP, UNKNOWN }

data class DiskSnapshot(val reading: DiskReading, val since: Long)

data class Snapshot(
    val now: Long,
    val networks: List<NetworkSnapshot>,
    /** How many more streams the waiting blocks could keep busy. */
    val spareWork: Int,
    /** Absent/null, the disk isn't judged. */
    val disk: DiskSnapshot? = null
)

sealed class Action {
    abstract val networkId: String
    abstract val count: Int

    data class Add(override val networkId: String, override val count: Int) : Action()
    data class Retire(override val networkId: String, override val count: Int) : Action()
}

/** Whether the disk is what holds a download back, judged by the clock. */
class DiskWatch {
    var reading: DiskReading = DiskReading.UNKNOWN
        private set
    var since: Long = 0
        private set
    private var landedAt: Long = 0

    /** A write reached the disk. */
    fun landed(now: Long) {
        landedAt = now
    }

    /** One look at the download: its streams fetching a block, and how many of
     * those hold bytes the disk hasn't taken. */
    fun sample(now: Long, writing: Int, held: Int) {
        val next = when {
            writing == 0 -> DiskReading.UNKNOWN
            held * 2 <= writing -> DiskReading.KEEPING_UP
            now - landedAt < DISK_STALLED_MS -> DiskReading.BEHIND
            else -> DiskReading.UNKNOWN
        }
        if (next == reading) return
        reading = next
        since = now
    }

    fun reset() {
        reading = DiskReading.UNKNOWN
        since = 0
    }
}

class ConcurrencyController(
    /** The most streams a network may run. */
    private val maxPerNetwork: Int,
    /** false: keep each network at maxPerNetwork rather than doubling to it. */
    private val grows: Boolean = true,
    /** The most a network may run for this download's share of it now. */
    private val share: (String) -> Int = { Int.MAX_VALUE }
) {
    /** Networks a server has refused on: the most streams each may run, and when. */
    private val ceilings = mutableMapOf<String, Ceiling>()
    /** The most streams any network may run while the disk can't keep up. */
    private var diskCap: Int? = null
    private var diskFrom: Long = 0

    private data class Ceiling(var limit: Int, var since: Long)

    fun tick(snapshot: Snapshot): List<Action> {
        val actions = mutableListOf<Action>()
        val now = snapshot.now
        var spare = snapshot.spareWork
        if (grows) judgeDisk(snapshot)
        val diskHolds = grows && snapshot.disk != null &&
            snapshot.disk.reading != DiskReading.KEEPING_UP
        for (network in snapshot.networks) {
            val (id, streams) = network
            val refusedNow = network.refused > 0 && network.served > 0
            val entry = ceilings[id]
            if (refusedNow) {
                val limit = maxOf(1, minOf(streams - network.refused, entry?.limit ?: Int.MAX_VALUE))
                ceilings[id] = Ceiling(limit, now)
            } else if (entry != null && network.refused == 0 && now - entry.since >= RECOVER_MS) {
                entry.limit++
                entry.since = now
                if (entry.limit >= maxPerNetwork) ceilings.remove(id)
            }
            val ceiling = limit(id)
            if (streams > ceiling) {
                actions.add(Action.Retire(id, streams - ceiling))
                continue
            }
            // Not while the server is turning requests away, or the disk isn't keeping up.
            if (network.refused > 0 || diskHolds || streams == 0 || network.answered < streams) {
                continue
            }
            val count = minOf(
                if (grows) streams else Int.MAX_VALUE,
                ceiling - streams,
                spare
            )
            if (count < 1) continue
            spare -= count
            actions.add(Action.Add(id, count))
        }
        return actions
    }

    /** The most streams a network may run now: the lowest of a server's ceiling,
     * the disk's cap and this download's share, and never more than the limit. */
    fun limit(networkId: String): Int = minOf(
        maxPerNetwork,
        share(networkId),
        ceilings[networkId]?.limit ?: Int.MAX_VALUE,
        diskCap ?: Int.MAX_VALUE
    )

    private fun judgeDisk(snapshot: Snapshot) {
        val disk = snapshot.disk ?: return
        val now = snapshot.now
        val streams = snapshot.networks.maxOfOrNull { it.streams } ?: 0
        val cap = diskCap
        val lasted = now - maxOf(disk.since, diskFrom)
        when {
            disk.reading == DiskReading.BEHIND && lasted >= DISK_PATIENCE_MS && streams > 1 ->
                diskCap = (streams + 1) / 2
            disk.reading == DiskReading.KEEPING_UP && cap != null && streams < cap ->
                // Keeping up below the cap says nothing about it: the wait starts
                // once the streams are back at it.
                return
            disk.reading == DiskReading.KEEPING_UP && cap != null && lasted >= DISK_RECOVER_MS ->
                diskCap = if (cap * 2 >= maxPerNetwork) null else cap * 2
            else -> return
        }
        diskFrom = now
    }
}
