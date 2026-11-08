package com.netmax.netbond.engine

// Shared state types for the download engine.
// Ported from Plexo's src/shared/types (MIT, (c) 2026 Anmol Kapil) - see NOTICE.

enum class BlockStatus { PENDING, DOWNLOADING, DONE, FAILED }

enum class StreamStatus { PENDING, CONNECTING, DOWNLOADING, RETIRING, DONE }

/**
 * One block of an HTTP download. [bytesDownloaded] is its frontier: how much of
 * it, from its first byte, is secured on disk.
 */
data class HttpBlockState(
    val index: Int,
    val rangeStart: Long,
    /** Inclusive last byte; null = open-ended to end of file (unknown length). */
    val rangeEnd: Long?,
    var status: BlockStatus = BlockStatus.PENDING,
    var bytesDownloaded: Long = 0,
    val bytesByInterface: MutableMap<String, Long> = mutableMapOf()
)

/** Snapshot of one stream for scheduling decisions. */
data class HttpStreamState(
    val id: Int,
    val interfaceId: String,
    val status: StreamStatus,
    val speedBytesPerSec: Double
)
