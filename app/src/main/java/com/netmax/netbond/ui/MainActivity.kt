package com.netmax.netbond.ui

import android.Manifest
import android.app.Activity
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.graphics.Typeface
import android.os.Build
import android.os.Bundle
import android.view.Gravity
import android.view.ViewGroup.LayoutParams.MATCH_PARENT
import android.view.ViewGroup.LayoutParams.WRAP_CONTENT
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.TextView
import android.widget.Toast
import com.netmax.netbond.dl.DownloadService
import com.netmax.netbond.net.NetworkBinder

/**
 * Scaffold UI: paste a link, see which networks are bonded, start the download.
 * The real UI (queue, per-network speeds, history) is milestone M3.
 */
class MainActivity : Activity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        if (Build.VERSION.SDK_INT >= 33 &&
            checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }

        val networks = NetworkBinder(this).available()
        val status = TextView(this).apply {
            text = if (networks.isEmpty()) {
                "No internet network found"
            } else {
                "Bonded networks:\n" + networks.joinToString("\n") {
                    "- ${it.label}" + if (it.metered) " (metered)" else ""
                }
            }
            textSize = 16f
            setPadding(48, 24, 48, 24)
        }

        val title = TextView(this).apply {
            text = "NetBond"
            textSize = 28f
            typeface = Typeface.DEFAULT_BOLD
            setTextColor(Color.BLACK)
            setPadding(48, 48, 48, 8)
        }

        val input = EditText(this).apply {
            hint = "Paste a download link"
            setSingleLine()
            layoutParams = LinearLayout.LayoutParams(MATCH_PARENT, WRAP_CONTENT).apply {
                marginStart = 48; marginEnd = 48
            }
        }

        val start = Button(this).apply {
            text = "Download on all networks"
            setOnClickListener {
                val url = input.text.toString().trim()
                if (!url.startsWith("http")) {
                    Toast.makeText(context, "Paste a full http(s) link first", Toast.LENGTH_SHORT).show()
                    return@setOnClickListener
                }
                val intent = Intent(context, DownloadService::class.java)
                    .setAction(DownloadService.ACTION_START)
                    .putExtra(DownloadService.EXTRA_URL, url)
                startForegroundService(intent)
                Toast.makeText(context, "Download started - see notification", Toast.LENGTH_SHORT).show()
            }
        }

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            gravity = Gravity.CENTER_HORIZONTAL
            addView(title)
            addView(status)
            addView(input)
            addView(start)
        }
        setContentView(root)
    }
}
