# NetBond

Download files faster on Android by combining Wi-Fi and mobile data at the
same time - no VPN, no root.

One download is cut into blocks, and each network (Wi-Fi, mobile data,
Ethernet over USB-C) fetches its own blocks in parallel. Faster networks
automatically take more of the file; slow chunks get raced on idle networks at
the end so one weak connection can't hold the whole download up.

**Status: early scaffold.** The core engine (block planning, multi-network
work scheduling, adaptive per-network concurrency) is implemented; the app is
not yet device-tested. See the roadmap below.

## How it works

- `engine/` - the pure decision logic: how a file is cut into blocks
  (`Plan`), which stream fetches what (`Scheduler`: primary blocks, hedged
  races, end-game splits), and how many streams each network runs
  (`Concurrency`: doubling on success, backing off on server refusals and slow
  disks). No I/O, no clock of its own - every rule is testable against exact
  situations.
- `net/NetworkBinder` - the Android side of "use two networks at once":
  `ConnectivityManager` + per-network `Network.openConnection` /
  `getSocketFactory()`, and `requestNetwork(TRANSPORT_CELLULAR)` to bring
  mobile data up alongside Wi-Fi.
- `dl/` - bound HTTP range requests (`ChunkFetcher`), the download loop
  (`DownloadEngine`) and the foreground service that keeps downloads alive
  (`DownloadService`).

### A note on expectations

Two networks only add speed when each reaches the internet through its own
connection. Wi-Fi plus mobile data works. Wi-Fi plus Ethernet from the same
router does not - both share one link. Speed also depends on the server
allowing multiple range requests.

## Roadmap

- [x] Engine core ported and scaffolded
- [ ] Device-tested download loop (pause/resume, saved progress)
- [ ] Real UI: queue, per-network live speeds, history
- [ ] Speed limits, data allowances, scheduling
- [ ] Share-sheet and browser-link capture

## Credit

The multi-connection engine logic is a Kotlin port of the pure logic from
[Plexo](https://github.com/anmolkapil/plexo) (MIT, (c) 2026 Anmol Kapil) - see
[NOTICE](NOTICE). NetBond is MIT-licensed.
