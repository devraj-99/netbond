package com.netmax.netbond.dl

import com.netmax.netbond.net.BoundNetwork
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URL

/**
 * Fetches one byte range of a file over one bound network.
 * NetBond's own transport code - Plexo's Node/Electron HTTP layer does not
 * apply on Android; the planning/scheduling logic it feeds is the ported part.
 */
object ChunkFetcher {

    class HttpStatusError(val status: Int, val retryAfterMs: Long?) :
        Exception("Unexpected status $status for range request") {
        /** Statuses worth waiting out, as curl's --retry treats transient. */
        val transient: Boolean
            get() = status in listOf(408, 429, 500, 502, 503, 504)
    }

    data class OpenedStream(
        val body: InputStream,
        val contentLength: Long,
        /** true when the server honoured the Range header. */
        val ranged: Boolean
    )

    /**
     * Opens [url] for bytes [rangeStart]..[rangeEnd] (inclusive; null = to EOF)
     * on [network]. The caller reads and closes [OpenedStream.body].
     */
    fun open(
        url: URL,
        network: BoundNetwork,
        rangeStart: Long,
        rangeEnd: Long?,
        timeoutMs: Int = 15_000
    ): OpenedStream {
        val conn = network.openConnection(url).apply {
            connectTimeout = timeoutMs
            readTimeout = timeoutMs
            instanceFollowRedirects = true
            if (rangeStart > 0 || rangeEnd != null) {
                val end = rangeEnd?.let { it.toString() } ?: ""
                setRequestProperty("Range", "bytes=$rangeStart-$end")
            }
        }
        when (val status = conn.responseCode) {
            HttpURLConnection.HTTP_OK ->
                return OpenedStream(conn.inputStream, conn.contentLengthLong, ranged = false)
            HttpURLConnection.HTTP_PARTIAL ->
                return OpenedStream(conn.inputStream, conn.contentLengthLong, ranged = true)
            else -> {
                val retryAfter = conn.getHeaderField("Retry-After")?.toLongOrNull()?.times(1000)
                conn.disconnect()
                throw HttpStatusError(status, retryAfter)
            }
        }
    }
}
