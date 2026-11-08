package com.netmax.netbond.dl

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.net.ConnectivityManager
import android.os.IBinder
import android.os.PowerManager
import com.netmax.netbond.net.NetworkBinder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import java.io.File
import java.net.URL

/**
 * Foreground service that owns running downloads. A foreground service with a
 * visible notification is the reliable way to keep long downloads alive on
 * Xiaomi/HyperOS and other aggressive OEM skins; without it the process is
 * killed in the background and bonded streams die with it.
 *
 * While a download runs it also holds a cellular-network request, so mobile
 * data stays up alongside Wi-Fi and both can be bonded.
 */
class DownloadService : Service() {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main)
    private var engine: DownloadEngine? = null
    private var wakeLock: PowerManager.WakeLock? = null
    private var cellularRequest: ConnectivityManager.NetworkCallback? = null
    private var latestText = "Starting download..."
    private var done = false

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_START -> {
                val url = intent.getStringExtra(EXTRA_URL) ?: return START_NOT_STICKY
                startForeground(NOTIFICATION_ID, notification(latestText, ongoing = true))
                acquireWakeLock()
                startDownload(url)
            }
            ACTION_PAUSE -> scope.launch { engine?.pause(); updateNotification("Paused", ongoing = true) }
            ACTION_RESUME -> {
                engine?.resume()
                updateNotification(latestText, ongoing = true)
            }
            ACTION_STOP -> {
                engine?.stop()
                stopSelf()
            }
        }
        return START_STICKY
    }

    private fun startDownload(urlText: String) {
        val binder = NetworkBinder(this)
        val networks = binder.available()
        val labels = java.util.concurrent.ConcurrentHashMap<String, String>()
        for (n in networks) labels[n.id] = n.label
        val dest = File(getExternalFilesDir(null) ?: filesDir, fileNameFrom(urlText))
        engine = DownloadEngine(
            scope = scope,
            url = URL(urlText),
            destination = dest,
            initialNetworks = networks,
            onProgress = { downloaded, total, perNetwork ->
                val mb = downloaded / (1024 * 1024)
                val totalMb = if (total > 0) " of ${total / (1024 * 1024)} MB" else ""
                val nets = perNetwork.entries.joinToString("  ") { (id, bytes) ->
                    "${labels[id] ?: "Net"}: ${bytes / (1024 * 1024)} MB"
                }
                latestText = "Downloaded $mb MB$totalMb" + if (nets.isNotEmpty()) "\n$nets" else ""
                updateNotification(latestText, ongoing = true)
            },
            onComplete = { file ->
                done = true
                updateNotification("Done: ${file.name}", ongoing = false)
                stopSelf()
            },
            onFailed = { reason ->
                done = true
                updateNotification("Failed: $reason", ongoing = false)
                stopSelf()
            }
        )
        engine?.start()
        // Bring mobile data up next to Wi-Fi for the length of the download;
        // when it arrives it joins the engine as another bonded network.
        cellularRequest = binder.requestCellular { network ->
            if (network != null) {
                binder.available().find { it.network == network }?.let { bound ->
                    labels[bound.id] = bound.label
                    engine?.addNetwork(bound)
                }
            }
        }
    }

    private fun fileNameFrom(urlText: String): String {
        val name = urlText.substringAfterLast('/').substringBefore('?')
        return if (name.isNotBlank() && name.length < 80) name else "download.bin"
    }

    private fun acquireWakeLock() {
        val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
        wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "netbond:download")
            .apply { acquire(12 * 60 * 60 * 1000L) }
    }

    override fun onDestroy() {
        engine?.stop()
        cellularRequest?.let { NetworkBinder(this).cancelCellularRequest(it) }
        wakeLock?.release()
        scope.cancel()
        super.onDestroy()
    }

    private fun notification(text: String, ongoing: Boolean): Notification {
        val nm = getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(
            NotificationChannel(CHANNEL_ID, "Downloads", NotificationManager.IMPORTANCE_LOW)
        )
        val open = PendingIntent.getActivity(
            this, 0,
            packageManager.getLaunchIntentForPackage(packageName),
            PendingIntent.FLAG_IMMUTABLE
        )
        val builder = Notification.Builder(this, CHANNEL_ID)
            .setContentTitle("NetBond")
            .setContentText(text)
            .setStyle(Notification.BigTextStyle().bigText(text))
            .setSmallIcon(
                if (done) android.R.drawable.stat_sys_download_done
                else android.R.drawable.stat_sys_download
            )
            .setContentIntent(open)
            .setOngoing(ongoing)
        if (!done && ongoing) {
            builder.addAction(
                Notification.Action.Builder(
                    null, "Pause",
                    PendingIntent.getService(
                        this, 1,
                        Intent(this, DownloadService::class.java).setAction(ACTION_PAUSE),
                        PendingIntent.FLAG_IMMUTABLE
                    )
                ).build()
            )
            builder.addAction(
                Notification.Action.Builder(
                    null, "Stop",
                    PendingIntent.getService(
                        this, 2,
                        Intent(this, DownloadService::class.java).setAction(ACTION_STOP),
                        PendingIntent.FLAG_IMMUTABLE
                    )
                ).build()
            )
        }
        return builder.build()
    }

    private fun updateNotification(text: String, ongoing: Boolean) {
        val nm = getSystemService(NotificationManager::class.java)
        nm.notify(NOTIFICATION_ID, notification(text, ongoing))
    }

    companion object {
        const val ACTION_START = "com.netmax.netbond.action.START"
        const val ACTION_PAUSE = "com.netmax.netbond.action.PAUSE"
        const val ACTION_RESUME = "com.netmax.netbond.action.RESUME"
        const val ACTION_STOP = "com.netmax.netbond.action.STOP"
        const val EXTRA_URL = "url"
        private const val CHANNEL_ID = "downloads"
        private const val NOTIFICATION_ID = 1
    }
}
