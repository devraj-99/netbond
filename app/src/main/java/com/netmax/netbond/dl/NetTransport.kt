package com.netmax.netbond.dl

import java.net.HttpURLConnection
import java.net.URL

/**
 * One network a download can bind streams to. On Android this is a BoundNetwork
 * (android.net.Network-backed); in JVM unit tests it is a plain connection, so
 * the whole engine runs off-device.
 */
interface NetTransport {
    val id: String
    val label: String
    fun openConnection(url: URL): HttpURLConnection
}
