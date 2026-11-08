package com.netmax.netbond.net

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import java.net.HttpURLConnection
import java.net.URL
import javax.net.SocketFactory

/**
 * One internet-connected Android network a download can bind streams to.
 *
 * Android lets an app hold several networks at once (Wi-Fi + mobile data) and
 * direct individual sockets to each - no VPN, no root:
 *   - Network.openConnection(url) opens an HttpURLConnection through that network
 *   - Network.getSocketFactory() gives a SocketFactory pinned to it
 * Mobile data is normally torn down while Wi-Fi is up, so it has to be asked for
 * with requestNetwork(TRANSPORT_CELLULAR) before it shows up here.
 */
class BoundNetwork(
    val network: Network,
    val id: String,
    val label: String,
    val metered: Boolean
) {
    fun openConnection(url: URL): HttpURLConnection =
        network.openConnection(url) as HttpURLConnection

    val socketFactory: SocketFactory
        get() = network.socketFactory
}

class NetworkBinder(private val context: Context) {

    private val cm: ConnectivityManager
        get() = context.getSystemService(ConnectivityManager::class.java)

    /** Every network currently up that can reach the internet. */
    fun available(): List<BoundNetwork> {
        val out = mutableListOf<BoundNetwork>()
        for (network in cm.allNetworks) {
            val caps = cm.getNetworkCapabilities(network) ?: continue
            if (!caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)) continue
            if (!caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED)) continue
            val label = when {
                caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) -> "Wi-Fi"
                caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) -> "Mobile data"
                caps.hasTransport(NetworkCapabilities.TRANSPORT_ETHERNET) -> "Ethernet"
                caps.hasTransport(NetworkCapabilities.TRANSPORT_USB) -> "USB"
                else -> "Network"
            }
            out.add(
                BoundNetwork(
                    network = network,
                    id = network.toString(),
                    label = label,
                    metered = !caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_NOT_METERED)
                )
            )
        }
        return out
    }

    /**
     * Asks the system to bring mobile data up alongside Wi-Fi for this app.
     * The network arrives (or not) on [onNetwork]. The request stays active
     * until [cancelCellularRequest] runs; keep it only while a download needs it.
     */
    fun requestCellular(onNetwork: (Network?) -> Unit): ConnectivityManager.NetworkCallback {
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) = onNetwork(network)
            override fun onLost(network: Network) = onNetwork(null)
            override fun onUnavailable() = onNetwork(null)
        }
        val request = NetworkRequest.Builder()
            .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
            .addTransportType(NetworkCapabilities.TRANSPORT_CELLULAR)
            .build()
        cm.requestNetwork(request, callback)
        return callback
    }

    fun cancelCellularRequest(callback: ConnectivityManager.NetworkCallback) {
        cm.unregisterNetworkCallback(callback)
    }
}
