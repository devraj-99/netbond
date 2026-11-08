package com.netmax.netbond

import com.netmax.netbond.engine.Action
import com.netmax.netbond.engine.AttemptView
import com.netmax.netbond.engine.BlockStatus
import com.netmax.netbond.engine.ConcurrencyController
import com.netmax.netbond.engine.DiskReading
import com.netmax.netbond.engine.DiskSnapshot
import com.netmax.netbond.engine.HttpBlockState
import com.netmax.netbond.engine.HttpStreamState
import com.netmax.netbond.engine.MIB
import com.netmax.netbond.engine.NetworkSnapshot
import com.netmax.netbond.engine.Requester
import com.netmax.netbond.engine.SchedulerPolicy
import com.netmax.netbond.engine.SchedulerState
import com.netmax.netbond.engine.Snapshot
import com.netmax.netbond.engine.StreamStatus
import com.netmax.netbond.engine.WorkKind
import com.netmax.netbond.engine.advanceBlock
import com.netmax.netbond.engine.pickWork
import com.netmax.netbond.engine.planBlocks
import com.netmax.netbond.engine.planDownload
import com.netmax.netbond.engine.PlanRequest
import com.netmax.netbond.engine.retractBlock
import com.netmax.netbond.engine.startingStreams
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class EngineLogicTest {

    private fun block(index: Int, start: Long, end: Long?) =
        HttpBlockState(index = index, rangeStart = start, rangeEnd = end)

    @Test
    fun `small file gets minimum-sized blocks`() {
        val plan = planDownload(PlanRequest(totalBytes = 10 * MIB, splittable = true, networkCount = 2))
        assertEquals(MIB, plan.blockSizeBytes)
        assertEquals(10, plan.blockCount)
    }

    @Test
    fun `unsplittable file is one block`() {
        val plan = planDownload(PlanRequest(totalBytes = 100 * MIB, splittable = false, networkCount = 4))
        assertEquals(1, plan.blockCount)
    }

    @Test
    fun `unknown size is one open-ended block`() {
        val blocks = planBlocks(0, MIB)
        assertEquals(1, blocks.size)
        assertNull(blocks[0].rangeEnd)
    }

    @Test
    fun `blocks tile the file exactly`() {
        val blocks = planBlocks(10 * MIB + 7, MIB)
        assertEquals(11, blocks.size)
        assertEquals(0, blocks[0].rangeStart)
        assertEquals(MIB - 1, blocks[0].rangeEnd)
        assertEquals(10 * MIB, blocks[10].rangeStart)
        assertEquals(10 * MIB + 6, blocks[10].rangeEnd)
    }

    @Test
    fun `starting streams never zero but capped by waiting blocks`() {
        assertEquals(1, startingStreams(waiting = 3, networkCount = 8))
        assertEquals(8, startingStreams(waiting = 100, networkCount = 1))
    }

    @Test
    fun `frontier credits networks and retracts`() {
        val b = block(0, 0, 99)
        assertEquals(40, advanceBlock(b, "wifi", 40))
        assertEquals(0, advanceBlock(b, "data", 30)) // behind the frontier
        assertEquals(20, advanceBlock(b, "data", 60))
        assertEquals(40L, b.bytesByInterface["wifi"])
        assertEquals(20L, b.bytesByInterface["data"])
        retractBlock(b, 50, blamed = "data")
        assertEquals(50L, b.bytesDownloaded)
        assertEquals(10L, b.bytesByInterface["data"])
        assertEquals(40L, b.bytesByInterface["wifi"])
    }

    private val policy = SchedulerPolicy(
        hedgeAfterMs = 4_000, maxHedgesPerBlock = 3, startupMs = 1_500, minSplitBytes = MIB
    )

    @Test
    fun `free stream takes the first waiting block`() {
        val state = SchedulerState(
            blocks = listOf(block(0, 0, 99), block(1, 100, 199)),
            streams = listOf(HttpStreamState(1, "wifi", StreamStatus.PENDING, 0.0)),
            attempts = emptyMap(), avoid = emptyMap(), hedgesUsed = emptyMap()
        )
        val work = pickWork(state, Requester(1, "wifi"), now = 10_000, policy)!!
        assertEquals(WorkKind.PRIMARY, work.kind)
        assertEquals(0, work.block.index)
    }

    @Test
    fun `slow block gets hedged once nothing waits`() {
        val slow = block(0, 0, 10 * MIB - 1).copy(status = BlockStatus.DOWNLOADING)
        slow.bytesDownloaded = MIB
        val state = SchedulerState(
            blocks = listOf(slow),
            streams = listOf(
                HttpStreamState(1, "wifi", StreamStatus.DOWNLOADING, 100.0), // 100 B/s
                HttpStreamState(2, "data", StreamStatus.PENDING, 1_000_000.0) // 1 MB/s
            ),
            attempts = mapOf(
                0 to listOf(
                    AttemptView(
                        kind = WorkKind.PRIMARY, streamId = 1, networkId = "wifi",
                        startedAt = 0, startOffset = 0, end = 10 * MIB, networkReceived = MIB
                    )
                )
            ),
            avoid = emptyMap(), hedgesUsed = emptyMap()
        )
        val work = pickWork(state, Requester(2, "data", 1_000_000.0), now = 10_000, policy)!!
        assertEquals(WorkKind.HEDGE, work.kind)
        assertEquals(0, work.block.index)
    }

    @Test
    fun `no hedge while blocks still wait`() {
        val slow = block(0, 0, 10 * MIB - 1).copy(status = BlockStatus.DOWNLOADING)
        slow.bytesDownloaded = MIB
        val waiting = block(1, 10 * MIB, 20 * MIB - 1)
        val state = SchedulerState(
            blocks = listOf(slow, waiting),
            streams = listOf(
                HttpStreamState(1, "wifi", StreamStatus.DOWNLOADING, 1.0),
                HttpStreamState(2, "data", StreamStatus.PENDING, 1_000_000.0)
            ),
            attempts = mapOf(
                0 to listOf(
                    AttemptView(
                        kind = WorkKind.PRIMARY, streamId = 1, networkId = "wifi",
                        startedAt = 0, startOffset = 0, end = 10 * MIB, networkReceived = MIB
                    )
                )
            ),
            avoid = emptyMap(), hedgesUsed = emptyMap()
        )
        val work = pickWork(state, Requester(2, "data", 1_000_000.0), now = 10_000, policy)!!
        assertEquals(WorkKind.PRIMARY, work.kind)
        assertEquals(1, work.block.index)
    }

    @Test
    fun `concurrency doubles an answered network and caps at the limit`() {
        val c = ConcurrencyController(maxPerNetwork = 8)
        val snap = { streams: Int, answered: Int ->
            Snapshot(
                now = 0,
                networks = listOf(NetworkSnapshot("wifi", streams, answered, refused = 0, served = answered)),
                spareWork = 100
            )
        }
        val actions = c.tick(snap(4, 4))
        assertEquals(listOf(Action.Add("wifi", 4)), actions)
        // At the limit: nothing more.
        assertTrue(c.tick(snap(8, 8)).isEmpty())
    }

    @Test
    fun `refused streams lower the ceiling until quiet`() {
        val c = ConcurrencyController(maxPerNetwork = 32)
        val refused = Snapshot(
            now = 1_000,
            networks = listOf(NetworkSnapshot("wifi", streams = 8, answered = 8, refused = 3, served = 5)),
            spareWork = 100
        )
        val actions = c.tick(refused)
        assertEquals(listOf(Action.Retire("wifi", 3)), actions)
        assertEquals(5, c.limit("wifi"))
    }

    @Test
    fun `disk behind halves streams and recovery doubles back`() {
        val c = ConcurrencyController(maxPerNetwork = 32)
        val behind = Snapshot(
            now = 10_000,
            networks = listOf(NetworkSnapshot("wifi", streams = 8, answered = 8, refused = 0, served = 8)),
            spareWork = 100,
            disk = DiskSnapshot(DiskReading.BEHIND, since = 9_000)
        )
        val actions = c.tick(behind)
        assertEquals(listOf(Action.Retire("wifi", 4)), actions)
    }
}
