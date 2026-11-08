package com.netmax.netbond.dl

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import com.netmax.netbond.net.BoundNetwork
import com.netmax.netbond.net.NetworkBinder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import java.io.File
import java.net.URL

/**
 * Foreground service that owns running downloads. A foreground service with a
 * visible notification is the reliable way to keep long downloads alive on
 * Xiaomi/HyperOS and other aggressive OEM skins; without it the process is
 * killed in the background and bonded streams die with it.
 */
class DownloadService : Service() {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main)
    private var engine: DownloadEngine? = null
    private var wakeLock: PowerManager.WakeLock? = null

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_START -> {
                val url = intent.getStringExtra(EXTRA_URL) ?: return START_NOT_STICKY
                startForeground(NOTIFICATION_ID, notification("Starting download..."))
                acquireWakeLock()
                startDownload(url)
            }
            ACTION_STOP -> stopSelf()
        }
        return START_STICKY
    }

    private fun startDownload(urlText: String) {
        val binder = NetworkBinder(this)
        val networks: List<BoundNetwork> = binder.available()
        val dest = File(getExternalFilesDir(null) ?: filesDir, "download.bin")
        engine = DownloadEngine(
            scope = scope,
            url = URL(urlText),
            destination = dest,
            networks = networks,
            onProgress = { downloaded, total, _ ->
                updateNotification("Downloaded ${downloaded / (1024 * 1024)} MB" +
                    if (total > 0) " of ${total / (1024 * 1024)} MB" else "")
                if (total > 0 && downloaded >= total) stopSelf()
            }
        )
        engine?.start()
    }

    private fun acquireWakeLock() {
        val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
        wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "netbond:download")
            .apply { acquire(12 * 60 * 60 * 1000L) }
    }

    override fun onDestroy() {
        engine?.stop()
        wakeLock?.release()
        scope.cancel()
        super.onDestroy()
    }

    private fun notification(text: String): Notification {
        val nm = getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(
            NotificationChannel(CHANNEL_ID, "Downloads", NotificationManager.IMPORTANCE_LOW)
        )
        val open = PendingIntent.getActivity(
            this, 0,
            packageManager.getLaunchIntentForPackage(packageName),
            PendingIntent.FLAG_IMMUTABLE
        )
        val builder = if (Build.VERSION.SDK_INT >= 31) {
            Notification.Builder(this, CHANNEL_ID)
        } else {
            Notification.Builder(this, CHANNEL_ID)
        }
        return builder
            .setContentTitle("NetBond")
            .setContentText(text)
            .setSmallIcon(android.R.drawable.stat_sys_download)
            .setContentIntent(open)
            .setOngoing(true)
            .build()
    }

    private fun updateNotification(text: String) {
        val nm = getSystemService(NotificationManager::class.java)
        nm.notify(NOTIFICATION_ID, notification(text))
    }

    companion object {
        const val ACTION_START = "com.netmax.netbond.action.START"
        const val ACTION_STOP = "com.netmax.netbond.action.STOP"
        const val EXTRA_URL = "url"
        private const val CHANNEL_ID = "downloads"
        private const val NOTIFICATION_ID = 1
    }
}
