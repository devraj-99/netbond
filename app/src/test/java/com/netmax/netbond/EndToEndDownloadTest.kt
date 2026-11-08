package com.netmax.netbond

import com.netmax.netbond.dl.DownloadEngine
import com.netmax.netbond.dl.NetTransport
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.BufferedInputStream
import java.io.File
import java.net.HttpURLConnection
import java.net.ServerSocket
import java.net.Socket
import java.net.URL
import java.security.MessageDigest
import java.util.Random
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

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

    /**
     * Minimal keep-alive HTTP/1.1 server with Range support, built on a raw
     * ServerSocket: com.sun.net.httpserver is not on Android's unit-test
     * compile classpath, and the plain JDK always is.
     */
    private class RangeServer(private val bytes: ByteArray) {
        private val socket = ServerSocket(0)
        @Volatile private var running = true
        val port: Int get() = socket.localPort

        fun start() {
            thread(isDaemon = true, name = "range-server-accept") {
                while (running) {
                    val client = try { socket.accept() } catch (e: Exception) { break }
                    thread(isDaemon = true, name = "range-server-conn") { serve(client) }
                }
            }
        }

        fun stop() {
            running = false
            try { socket.close() } catch (_: Exception) {}
        }

        private fun readLine(input: BufferedInputStream): String? {
            val buf = StringBuilder()
            while (true) {
                val c = input.read()
                if (c == -1) return if (buf.isEmpty()) null else buf.toString()
                if (c == '\n'.code) return buf.trimEnd('\r').toString()
                buf.append(c.toChar())
            }
        }

        private fun serve(client: Socket) {
            try {
                client.soTimeout = 30_000
                val input = BufferedInputStream(client.getInputStream())
                val output = client.getOutputStream()
                while (true) {
                    val requestLine = readLine(input) ?: break
                    if (requestLine.isEmpty()) break
                    val parts = requestLine.split(" ")
                    if (parts.size < 2) break
                    val method = parts[0]
                    val headers = mutableMapOf<String, String>()
                    while (true) {
                        val line = readLine(input) ?: break
                        if (line.isEmpty()) break
                        val colon = line.indexOf(':')
                        if (colon > 0) headers[line.substring(0, colon).trim().lowercase()] =
                            line.substring(colon + 1).trim()
                    }
                    val range = headers["range"]?.let {
                        Regex("bytes=(\\d+)-(\\d*)").find(it)
                    }
                    val head = method == "HEAD"
                    val body: ByteArray
                    val status: String
                    val extra = StringBuilder()
                    if (range != null) {
                        val startByte = range.groupValues[1].toInt()
                        val endByte = if (range.groupValues[2].isEmpty()) bytes.size - 1
                        else minOf(range.groupValues[2].toInt(), bytes.size - 1)
                        body = bytes.copyOfRange(startByte, endByte + 1)
                        status = "206 Partial Content"
                        extra.append("Content-Range: bytes $startByte-$endByte/${bytes.size}\r\n")
                    } else {
                        body = bytes
                        status = "200 OK"
                    }
                    val head_ = StringBuilder()
                    head_.append("HTTP/1.1 $status\r\n")
                    head_.append("Content-Length: ${body.size}\r\n")
                    head_.append("Accept-Ranges: bytes\r\n")
                    head_.append(extra)
                    head_.append("Connection: keep-alive\r\n\r\n")
                    output.write(head_.toString().toByteArray(Charsets.US_ASCII))
                    if (!head) output.write(body)
                    output.flush()
                }
            } catch (_: Exception) {
                // Client went away mid-download (pause/stop tests do this); fine.
            } finally {
                try { client.close() } catch (_: Exception) {}
            }
        }
    }

    private lateinit var server: RangeServer
    private lateinit var fileBytes: ByteArray
    private lateinit var dir: File
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @Before
    fun setUp() {
        fileBytes = ByteArray(12 * 1024 * 1024).also { Random(42).nextBytes(it) }
        dir = createTempDir("netbond-test")
        server = RangeServer(fileBytes)
        server.start()
    }

    @After
    fun tearDown() {
        server.stop()
        dir.deleteRecursively()
    }

    private fun url(): URL = URL("http://127.0.0.1:${server.port}/file.bin")

    @Test
    fun `downloads a file across two networks byte-perfect`() {
        val dest = File(dir, "out.bin")
        val done = CountDownLatch(1)
        var perNetwork = mapOf<String, Long>()
        var failure = ""

        val engine = DownloadEngine(
            scope = scope,
            url = url(),
            destination = dest,
            initialNetworks = listOf(FakeTransport("wifi"), FakeTransport("mobile")),
            onProgress = { _, _, per -> perNetwork = per },
            onComplete = { done.countDown() },
            onFailed = { reason -> failure = reason; done.countDown() }
        )
        engine.start()

        assertTrue(
            "did not finish in 60s (failure=$failure)",
            done.await(60, TimeUnit.SECONDS)
        )
        assertTrue("failed: $failure", failure.isEmpty())
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
        var failure = ""
        val engine = DownloadEngine(
            scope = scope, url = url(), destination = dest,
            initialNetworks = listOf(FakeTransport("wifi")),
            onProgress = { _, _, _ -> },
            onComplete = { done.countDown() },
            onFailed = { failure = it; done.countDown() }
        )
        engine.start()
        assertTrue("failed: $failure", done.await(60, TimeUnit.SECONDS))
        val want = MessageDigest.getInstance("SHA-256").digest(fileBytes)
        val got = MessageDigest.getInstance("SHA-256").digest(dest.readBytes())
        assertArrayEquals(want, got)
    }
}
