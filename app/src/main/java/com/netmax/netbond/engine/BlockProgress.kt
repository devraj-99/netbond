package com.netmax.netbond.engine

// A block's bytesDownloaded is its frontier: how much of it, from its first byte,
// is secured. Near the end of a download two attempts can race for one block, so
// the frontier is the furthest either has reached, not the sum of what they
// received. bytesByInterface splits it by network and always adds up to it.
// Ported from Plexo's src/main/download/blockProgress.ts (MIT, (c) 2026 Anmol Kapil) - see NOTICE.

private fun credit(block: HttpBlockState, networkId: String, bytes: Long) {
    block.bytesByInterface[networkId] = (block.bytesByInterface[networkId] ?: 0) + bytes
}

/** Drops attribution for bytes that turned out not to be secured. The lost bytes
 * are always at the tail, so they come off [blamed] first before spilling over. */
private fun trim(block: HttpBlockState, keepBytes: Long, blamed: String?) {
    var excess = block.bytesByInterface.values.sum() - keepBytes
    if (excess <= 0) return
    val order = block.bytesByInterface.keys.sortedWith { a, b ->
        when {
            a == blamed -> -1
            b == blamed -> 1
            else -> 0
        }
    }
    for (networkId in order) {
        if (excess <= 0) break
        val taken = minOf(block.bytesByInterface[networkId] ?: 0, excess)
        val remaining = (block.bytesByInterface[networkId] ?: 0) - taken
        excess -= taken
        if (remaining > 0) block.bytesByInterface[networkId] = remaining
        else block.bytesByInterface.remove(networkId)
    }
}

/** Moves the frontier forward to [position] (a byte offset into the block),
 * crediting the gain to [networkId]. Does nothing if something already got
 * further. Returns the gain. */
fun advanceBlock(block: HttpBlockState, networkId: String, position: Long): Long {
    val length = block.rangeEnd?.let { it - block.rangeStart + 1 } ?: Long.MAX_VALUE
    val next = minOf(position, length)
    val gained = next - block.bytesDownloaded
    if (gained <= 0) return 0
    block.bytesDownloaded = next
    credit(block, networkId, gained)
    return gained
}

/** Pulls the frontier back to [position], for bytes that turned out not to be
 * there or a racing attempt that lost. Returns how much it removed. */
fun retractBlock(block: HttpBlockState, position: Long, blamed: String? = null): Long {
    val removed = block.bytesDownloaded - position
    if (removed <= 0) return 0
    trim(block, position, blamed)
    block.bytesDownloaded = position
    return removed
}
