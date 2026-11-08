package com.netmax.netbond

import com.netmax.netbond.dl.DownloadEngine
import com.netmax.netbond.dl.NetTransport
import com.sun.net.httpserver.HttpServer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.net.HttpURLConnection
import java.net.InetSocketAddress
import java.net.URL
import java.security.MessageDigest
import java.util.Random
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Real end-to-end run of the download engine on the JVM: a local HTTP server
 * with Range support serves a file to two fake "networks" (plain connections),
 * and the engine's own scheduling spreads blocks across both. Loopback is one
 * physical link, so this verifies correctness of the loop and the per-network
 * code path - not a real WiFi+cellular speedup.
 */
class EndToEndDownloadTest {

    private class FakeTransport(override val id: String) : NetTransport {
        override val label: String = id
        override fun openConnection(url: URL): HttpURLConnection =
            url.openConnection() as HttpURLConnection
    }

    private lateinit var server: HttpServer
    private lateinit var fileBytes: ByteArray
    private lateinit var dir: File
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @Before
    fun setUp() {
        fileBytes = ByteArray(12 * 1024 * 1024).also { Random(42).nextBytes(it) }
        dir = createTempDir("netbond-test")
        server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/file.bin") { exchange ->
                val range = exchange.requestHeaders.getFirst("Range")
                if (exchange.requestMethod == "HEAD") {
                    exchange.responseHeaders.add("Accept-Ranges", "bytes")
                    exchange.sendResponseHeaders(200, fileBytes.size.toLong())
                    exchange.close()
                    return@createContext
                }
                if (range != null) {
                    val m = Regex("bytes=(\\d+)-(\\d*)").find(range)!!
                    val start = m.groupValues[1].toInt()
                    val end = if (m.groupValues[2].isEmpty()) fileBytes.size - 1
                    else minOf(m.groupValues[2].toInt(), fileBytes.size - 1)
                    val len = end - start + 1
                    exchange.responseHeaders.add("Accept-Ranges", "bytes")
                    exchange.responseHeaders.add("Content-Range", "bytes $start-$end/${fileBytes.size}")
                    exchange.sendResponseHeaders(206, len.toLong())
                    exchange.responseBody.use { it.write(fileBytes, start, len) }
                } else {
                    exchange.responseHeaders.add("Accept-Ranges", "bytes")
                    exchange.sendResponseHeaders(200, fileBytes.size.toLong())
                    exchange.responseBody.use { it.write(fileBytes) }
                }
                exchange.close()
            }
            start()
        }
    }

    @After
    fun tearDown() {
        server.stop(0)
        dir.deleteRecursively()
    }

    private fun url(): URL = URL("http://127.0.0.1:${server.address.port}/file.bin")

    @Test
    fun `downloads a file across two networks byte-perfect`() {
        val dest = File(dir, "out.bin")
        val done = CountDownLatch(1)
        val failed = CountDownLatch(1)
        var perNetwork = mapOf<String, Long>()
        var failure = ""

        val engine = DownloadEngine(
            scope = scope,
            url = url(),
            destination = dest,
            initialNetworks = listOf(FakeTransport("wifi"), FakeTransport("mobile")),
            onProgress = { _, _, per -> perNetwork = per },
            onComplete = { done.countDown() },
            onFailed = { reason -> failure = reason; failed.countDown() }
        )
        engine.start()

        assertTrue(
            "did not finish in 60s (failure=$failure)",
            done.await(60, TimeUnit.SECONDS)
        )
        assertArrayEquals(fileBytes, dest.readBytes())
        assertEquals(fileBytes.size.toLong(), dest.length())
        // Both networks took part: the whole point of the engine.
        assertTrue("wifi carried nothing: $perNetwork", (perNetwork["wifi"] ?: 0) > 0)
        assertTrue("mobile carried nothing: $perNetwork", (perNetwork["mobile"] ?: 0) > 0)
        val credited = perNetwork.values.sum()
        assertEquals(fileBytes.size.toLong(), credited)
    }

    @Test
    fun `pause saves progress and a fresh engine resumes it`() {
        val dest = File(dir, "resume.bin")
        val done = CountDownLatch(1)
        var failure = ""

        val first = DownloadEngine(
            scope = scope, url = url(), destination = dest,
            initialNetworks = listOf(FakeTransport("wifi"), FakeTransport("mobile")),
            onProgress = { _, _, _ -> },
            onComplete = { },
            onFailed = { failure = it }
        )
        first.start()
        Thread.sleep(800) // let some blocks land
        kotlinx.coroutines.runBlocking { first.pause() }
        first.stop()

        val sidecar = File(dir, "resume.bin.netbond.json")
        assertTrue("no progress saved", sidecar.exists())
        val savedFrontier = Regex("\"frontier\":(\\d+)").findAll(sidecar.readText())
            .sumOf { it.groupValues[1].toLong() }
        assertTrue("nothing was saved", savedFrontier > 0)

        val second = DownloadEngine(
            scope = scope, url = url(), destination = dest,
            initialNetworks = listOf(FakeTransport("wifi"), FakeTransport("mobile")),
            onProgress = { _, _, _ -> },
            onComplete = { done.countDown() },
            onFailed = { failure = it; done.countDown() }
        )
        second.start()
        assertTrue("resume did not finish in 60s (failure=$failure)", done.await(60, TimeUnit.SECONDS))
        assertTrue(failure, failure.isEmpty())
        assertArrayEquals(fileBytes, dest.readBytes())
    }

    @Test
    fun `sha sanity - file content matches exactly`() {
        // Guards against off-by-one corruption in block tiling.
        val dest = File(dir, "sha.bin")
        val done = CountDownLatch(1)
        val engine = DownloadEngine(
            scope = scope, url = url(), destination = dest,
            initialNetworks = listOf(FakeTransport("wifi")),
            onProgress = { _, _, _ -> },
            onComplete = { done.countDown() }
        )
        engine.start()
        assertTrue(done.await(60, TimeUnit.SECONDS))
        val want = MessageDigest.getInstance("SHA-256").digest(fileBytes)
        val got = MessageDigest.getInstance("SHA-256").digest(dest.readBytes())
        assertArrayEquals(want, got)
    }
}
