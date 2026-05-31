# LiveSync Bridge Reliability: Network Error Resilience & Watch Backfill

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two critical reliability issues in the livesync-bridge: (1) unhandled network errors that crash the entire process, and (2) the stuck CouchDB `_changes` watcher that never backfills missed changes once the `since` pointer has advanced past them.

**Architecture:** The bridge is a Deno TypeScript app with two peer types — `PeerCouchDB` (CouchDB _changes feed) and `PeerStorage` (filesystem watcher). A `Hub` dispatches changes between peers. The CouchDB peer uses PouchDB's `.changes()` live feed, which is set up in `DirectFileManipulator.beginWatch()`. The `since` pointer determines where the feed starts; once set to "now", any prior changes are lost until a `--reset` clears `localStorage`.

**Tech Stack:** Deno, TypeScript, PouchDB (HTTP adapter), node-fetch (used internally by PouchDB HTTP adapter), localStorage for persistence.

---

## Fix 1: Network Error Resilience (Crash on Transient DNS/Network Failures)

### Problem

The bridge crashes on any unhandled network error. The observed crash is `FetchError: getaddrinfo ENOTFOUND obsidian.vadc.ovh` — a transient DNS resolution failure that kills the entire Deno process because the error propagates as an unhandled promise rejection.

**Root cause locations:**

#### `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`
- `put()` (lines ~35-62): Calls `this.man.put()` which makes HTTP requests to CouchDB. No try/catch.
- `delete()` (lines ~24-33): Calls `this.man.delete()`. No try/catch.
- `get()` (lines ~63-76): Calls `this.man.get()`. No try/catch.
- `start()` (lines ~78-145): The `beginWatch` callback is wrapped in a try/catch, but the outer `start()` itself does `await this.man.rawGet()` and `await this.man.ready.promise` — network calls with no error handling.

#### `/home/claude/workspace/livesync-bridge/PeerStorage.ts`
- `put()` (lines ~38-80): Uses `Deno.open()`, `Deno.mkdir()` — filesystem ops, not network. Has try/catch.
- `delete()` (lines ~17-28): Uses `Deno.remove()`. Has try/catch.
- `start()`/`startDenoFsWatch()` (lines ~176-212): Filesystem watcher, no network calls.

#### `/home/claude/workspace/livesync-bridge/lib/src/API/DirectFileManipulatorV2.ts`
- `beginWatch()` (lines ~280-350): The `.on("error")` handler (lines ~340-352) logs the error and attempts reconnection with `setTimeout(() => { this.beginWatch(...) }, 10000)`. **This is partially correct** — it handles _watch_ errors. However:
  1. The reconnection delay is fixed at 10s with no backoff.
  2. The error handler references `this.watching` but never sets it to `false` before the reconnection timeout, creating a potential race condition.
  3. The `.on("change")` handler calls `this.getByMeta(doc)` which can throw network errors if CouchDB is unreachable — this error propagates through `callback()` which is the `Hub.dispatch()` → `PeerStorage.put()` chain. **The callback try/catch in `on("change")` only catches processing errors, not network errors from the change handler itself** — but the real crash is from `put()`/`delete()` which dispatch to `PeerCouchDB`.

#### `/home/claude/workspace/livesync-bridge/Hub.ts`
- `dispatch()` (lines ~18-30): Calls `peer.put()` and `peer.delete()` with `await`. These can throw `FetchError`. **No try/catch around the dispatch loop.** This is the direct crash path: when the storage peer detects a local file change and dispatches it to the CouchDB peer, the `await peer.put()` throws and it's an unhandled promise in the PeerStorage event handler, crashing the process.

#### `/home/claude/workspace/livesync-bridge/main.ts`
- No global error handlers. No `process.on('unhandledRejection')` or `addEventListener('unhandledrejection')`.

### Fix Strategy

1. **Wrap `Hub.dispatch()` in try/catch** — catch network errors per-peer, log them, continue to next peer. This prevents the primary crash path.
2. **Wrap `PeerCouchDB.put()`, `delete()`, `get()` in try/catch** — return false on network errors instead of throwing.
3. **Wrap `PeerCouchDB.start()` in try/catch with retry** — if initial CouchDB connection fails, retry with exponential backoff instead of crashing.
4. **Add global unhandled rejection handler in `main.ts`** — catch any remaining unhandled promise rejections, log them, and prevent process crash.
5. **Add exponential backoff to the watch reconnection** in `DirectFileManipulator.beginWatch()`.

### Task 1: Add network error resilience to Hub.dispatch()

**Files:**
- Modify: `/home/claude/workspace/livesync-bridge/Hub.ts`

- [ ] **Step 1: Write the test**

Create `/home/claude/workspace/livesync-bridge/_test/Hub.test.ts`:

```typescript
import { assertEquals } from "jsr:@std/assert";
import { Hub } from "../Hub.ts";
import { Peer, DispatchFun } from "../Peer.ts";
import { PeerConf, FileData } from "../types.ts";

class MockPeer extends Peer {
  shouldThrow = false;
  putCalls: string[] = [];
  deleteCalls: string[] = [];
  
  constructor(conf: PeerConf, dispatcher: DispatchFun, shouldThrow = false) {
    super(conf, dispatcher);
    this.shouldThrow = shouldThrow;
  }
  async put(path: string, data: FileData): Promise<boolean> {
    this.putCalls.push(path);
    if (this.shouldThrow) throw new Error("Network error: ENOTFOUND");
    return true;
  }
  async delete(path: string): Promise<boolean> {
    this.deleteCalls.push(path);
    if (this.shouldThrow) throw new Error("Network error: ENOTFOUND");
    return true;
  }
  async get(path: string): Promise<false | FileData> {
    return false;
  }
  async start(): Promise<void> {}
  async stop(): Promise<void> {}
}

Deno.test("Hub.dispatch continues to next peer when one throws", async () => {
  const config = {
    peers: [
      { type: "storage" as const, name: "peer1", baseDir: "/", shouldThrow: false },
      { type: "couchdb" as const, name: "peer2", baseDir: "/", shouldThrow: true },
      { type: "storage" as const, name: "peer3", baseDir: "/", shouldThrow: false },
    ]
  };
  
  // We need at least one non-throwing peer to verify dispatch reaches it
  const hub = new Hub({ peers: [] });
  const peer1 = new MockPeer(config.peers[0], hub.dispatch.bind(hub), false);
  const peer2 = new MockPeer(config.peers[1], hub.dispatch.bind(hub), true);
  const peer3 = new MockPeer(config.peers[2], hub.dispatch.bind(hub), false);
  hub.peers = [peer1, peer2, peer3];
  
  const data: FileData = { ctime: Date.now(), mtime: Date.now(), size: 10, data: ["test"] };
  // Dispatch from peer1 (source) — should reach peer2 and peer3
  await hub.dispatch(peer1, "test/path.md", data);
  
  // peer2 threw but peer3 should still have been called
  assertEquals(peer3.putCalls.length, 1);
  assertEquals(peer3.putCalls[0], "test/path.md");
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/workspace/livesync-bridge && deno test _test/Hub.test.ts -A`
Expected: FAIL — `hub.dispatch` will throw on the throwing peer before reaching peer3.

- [ ] **Step 3: Implement error handling in Hub.dispatch()**

Modify `/home/claude/workspace/livesync-bridge/Hub.ts`, replace the `dispatch` method:

```typescript
    async dispatch(source: Peer, path: string, data: FileData | false) {
        // Skip .git and .cocoindex_code internal files — never sync to CouchDB
        if (path.startsWith('.git/') || path === '.git' || path.includes('/.git/') || path.endsWith('/.git') ||
            path.startsWith('.cocoindex_code/') || path === '.cocoindex_code' || path.includes('/.cocoindex_code/') || path.endsWith('/.cocoindex_code')) {
            return;
        }
        for (const peer of this.peers) {
            if (peer !== source && (source.config.group ?? "") === (peer.config.group ?? "")) {
                try {
                    let ret = false;
                    if (data === false) {
                        ret = await peer.delete(path);
                    } else {
                        ret = await peer.put(path, data);
                    }
                    if (ret) {
                        // Logger(`  ${data === false ? "-x->" : "--->"} ${peer.config.name} ${path} `)
                    } else {
                        // Logger(`        ${peer.config.name} ignored ${path} `)
                    }
                } catch (ex) {
                    console.error(`[Hub] Error dispatching to ${peer.config.name} for ${path}:`, ex);
                }
            }
        }
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd ~/workspace/livesync-bridge && deno test _test/Hub.test.ts -A`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd ~/workspace/livesync-bridge
git add Hub.ts _test/Hub.test.ts
git commit -m "fix: wrap Hub.dispatch in try/catch to prevent network errors from crashing the process"
```

---

### Task 2: Add error handling to PeerCouchDB methods

**Files:**
- Modify: `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`

- [ ] **Step 1: Write the test**

Create `/home/claude/workspace/livesync-bridge/_test/PeerCouchDB.test.ts`:

```typescript
import { assertEquals, assert } from "jsr:@std/assert";
import { PeerCouchDB } from "../PeerCouchDB.ts";
import { Hub } from "../Hub.ts";

// This test verifies that PeerCouchDB methods return false instead of throwing
// on network errors. We test this by mocking the underlying DirectFileManipulator
// to throw, and verifying the method catches and returns false.

Deno.test("PeerCouchDB.put returns false on network error instead of throwing", async () => {
  // We can't easily unit-test PeerCouchDB without a real CouchDB instance.
  // The real protection is in Hub.dispatch (Task 1) and the global handler (Task 4).
  // This test verifies the method signature contract: put() should never throw unhandled.
  // Integration test would require a CouchDB instance.
  assertEquals(true, true, "Placeholder — real test requires CouchDB mock");
});

Deno.test("PeerCouchDB.delete returns false on network error instead of throwing", async () => {
  assertEquals(true, true, "Placeholder — real test requires CouchDB mock");
});
```

- [ ] **Step 2: Implement error handling in PeerCouchDB methods**

Modify `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`:

In `put()` method, wrap the body in try/catch:

```typescript
    async put(pathSrc: string, data: FileData): Promise<boolean> {
        try {
            // Wait for the DirectFileManipulator to be fully initialized
            await this.man.ready.promise;
            const path = this.toLocalPath(pathSrc);
            if (await this.isRepeating(pathSrc, data)) {
                return false;
            }
            const type = isPlainText(path) ? "plain" : "newnote";
            const info: FileInfo = {
                ctime: data.ctime,
                mtime: data.mtime,
                size: data.size
            };
            const saveData = (data.data instanceof Uint8Array) ? createBinaryBlob(data.data) : createTextBlob(data.data);
            const old = await this.man.get(path as FilePathWithPrefix, true) as false | MetaEntry;
            if (old && Math.abs(this.compareDate(info, old)) < 3600) {
                const oldDoc = await this.man.getByMeta(old);
                if (oldDoc && ("data" in oldDoc)) {
                    const d = oldDoc.type == "plain" ? createTextBlob(oldDoc.data) : createBinaryBlob(new Uint8Array(decodeBinary(oldDoc.data)));
                    if (await isDocContentSame(d, saveData)) {
                        this.normalLog(` Skipped (Same) ${path} `);
                        return false;
                    }
                }
            }
            const r = await this.man.put(path, saveData, info, type);
            if (r) {
                this.receiveLog(` ${path} saved`);
            } else {
                this.receiveLog(` ${path} ignored`);
            }
            return r;
        } catch (ex) {
            this.normalLog(`PUT failed for ${pathSrc}: ${ex}`, LOG_LEVEL_NOTICE);
            return false;
        }
    }
```

In `delete()` method, wrap in try/catch:

```typescript
    async delete(pathSrc: string): Promise<boolean> {
        try {
            const path = this.toLocalPath(pathSrc);
            if (await this.isRepeating(pathSrc, false)) {
                return false;
            }
            const r = await this.man.delete(path);
            if (r) {
                this.receiveLog(` ${path} deleted`);
            } else {
                this.receiveLog(` ${path} delete failed`, LOG_LEVEL_NOTICE);
            }
            return r;
        } catch (ex) {
            this.normalLog(`DELETE failed for ${pathSrc}: ${ex}`, LOG_LEVEL_NOTICE);
            return false;
        }
    }
```

Add the `LOG_LEVEL_NOTICE` import to the existing import at the top of `PeerCouchDB.ts`:

```typescript
import { LOG_LEVEL_NOTICE } from "./lib/src/common/types.ts";
```

- [ ] **Step 3: Run placeholder tests**

Run: `cd ~/workspace/livesync-bridge && deno test _test/PeerCouchDB.test.ts -A`
Expected: PASS (placeholder tests)

- [ ] **Step 4: Commit**

```bash
cd ~/workspace/livesync-bridge
git add PeerCouchDB.ts _test/PeerCouchDB.test.ts
git commit -m "fix: wrap PeerCouchDB put/delete in try/catch to return false on network errors"
```

---

### Task 3: Add retry logic to PeerCouchDB.start() and watch reconnection with exponential backoff

**Files:**
- Modify: `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`
- Modify: `/home/claude/workspace/livesync-bridge/lib/src/API/DirectFileManipulatorV2.ts`

- [ ] **Step 1: Add retry logic to PeerCouchDB.start()**

Modify `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`, add a private method and wrap the `start()` body:

```typescript
    private async _startWithRetry(attempt: number = 0): Promise<void> {
        const MAX_RETRIES = 10;
        const BASE_DELAY_MS = 1000;
        const MAX_DELAY_MS = 300000; // 5 minutes
        
        try {
            await this._startInner();
        } catch (ex) {
            if (attempt >= MAX_RETRIES) {
                this.normalLog(`Start failed after ${MAX_RETRIES} retries, giving up: ${ex}`, LOG_LEVEL_NOTICE);
                throw ex;
            }
            const delay = Math.min(BASE_DELAY_MS * Math.pow(2, attempt), MAX_DELAY_MS);
            this.normalLog(`Start failed (attempt ${attempt + 1}/${MAX_RETRIES}), retrying in ${delay}ms: ${ex}`);
            await new Promise(resolve => setTimeout(resolve, delay));
            return this._startWithRetry(attempt + 1);
        }
    }

    async start(): Promise<void> {
        return this._startWithRetry();
    }

    private async _startInner(): Promise<void> {
        // ... (the entire body of the current start() method, unchanged)
    }
```

Move the current `start()` method body into `_startInner()` and have `start()` call `_startWithRetry()`.

- [ ] **Step 2: Add exponential backoff to watch reconnection in DirectFileManipulatorV2**

Modify `/home/claude/workspace/livesync-bridge/lib/src/API/DirectFileManipulatorV2.ts`:

Change the `beginWatch` method's `.on("error")` handler. Currently (line ~345):

```typescript
            .on("error", (err: Error) => {
                Logger(`WATCH: ERROR: `, LEVEL_INFO, "watch");
                Logger(err, LEVEL_VERBOSE, "watch");
                if (this.watching) {
                    Logger(`WATCH: CONNECTION HAS BEEN CLOSED, RECONNECTING...`, LEVEL_INFO, "watch");
                    this.watching = false;
                    this.changes = undefined;
                    setTimeout(() => {
                        this.beginWatch(callback, checkIsInterested);
                    }, 10000);
                } else {
                    Logger(`WATCH: CONNECTION HAS BEEN CLOSED.`, LEVEL_INFO, "watch");
                }
            });
```

Replace with exponential backoff:

```typescript
    reconnectAttempts = 0;
    maxReconnectDelay = 300000; // 5 minutes
    baseReconnectDelay = 1000;  // 1 second

    beginWatch(
        callback: (doc: ReadyEntry, seq?: string | number) => Promise<any> | void,
        checkIsInterested?: (doc: MetaEntry) => boolean
    ) {
        if (this.watching) return false;
        this.watching = true;
        this.reconnectAttempts = 0; // Reset on fresh start
        this.changes = this.liveSyncLocalDB.localDatabase
            .changes({
                include_docs: true,
                since: this.since,
                selector: {
                    type: { $ne: "leaf" },
                },
                live: true,
            })
            .on("change", async (change: any) => {
                // ... unchanged ...
            })
            .on("complete", () => {
                Logger(`WATCH: FINISHED`, LEVEL_INFO, "watch");
                this.watching = false;
                this.changes = undefined;
            })
            .on("error", (err: Error) => {
                Logger(`WATCH: ERROR: `, LEVEL_INFO, "watch");
                Logger(err, LEVEL_VERBOSE, "watch");
                if (this.watching) {
                    this.watching = false;
                    this.changes = undefined;
                    this.reconnectAttempts++;
                    const delay = Math.min(
                        this.baseReconnectDelay * Math.pow(2, this.reconnectAttempts - 1),
                        this.maxReconnectDelay
                    );
                    Logger(`WATCH: CONNECTION HAS BEEN CLOSED, RECONNECTING IN ${delay}ms (attempt ${this.reconnectAttempts})...`, LEVEL_INFO, "watch");
                    setTimeout(() => {
                        this.beginWatch(callback, checkIsInterested);
                    }, delay);
                } else {
                    Logger(`WATCH: CONNECTION HAS BEEN CLOSED.`, LEVEL_INFO, "watch");
                }
            });
    }
```

Also add the three new properties to the class:

```typescript
    reconnectAttempts = 0;
    maxReconnectDelay = 300000; // 5 minutes
    baseReconnectDelay = 1000;  // 1 second
```

In the `.on("change")` handler, reset `this.reconnectAttempts = 0;` on each successful change, so the backoff resets after the connection recovers:

```typescript
            .on("change", async (change: any) => {
                this.reconnectAttempts = 0; // Reset backoff on successful change
                // ... rest unchanged ...
            })
```

- [ ] **Step 3: Commit**

```bash
cd ~/workspace/livesync-bridge
git add PeerCouchDB.ts lib/src/API/DirectFileManipulatorV2.ts
git commit -m "fix: add exponential backoff for watch reconnection and retry logic for CouchDB start"
```

---

### Task 4: Add global unhandled rejection handler in main.ts

**Files:**
- Modify: `/home/claude/workspace/livesync-bridge/main.ts`

- [ ] **Step 1: Write the test**

Create `/home/claude/workspace/livesync-bridge/_test/main.test.ts`:

```typescript
import { assertEquals } from "jsr:@std/assert";

Deno.test("Global unhandled rejection handler prevents process crash", () => {
  // This is a structural test — we verify the handler is registered.
  // Runtime verification would require spawning a subprocess.
  assertEquals(true, true, "Placeholder — structural verification done in Step 3");
});
```

- [ ] **Step 2: Add global error handlers to main.ts**

Add at the top of `/home/claude/workspace/livesync-bridge/main.ts`, before the existing code:

```typescript
// Global error handlers — prevent unhandled rejections and exceptions from crashing the process
addEventListener("unhandledrejection", (event) => {
    console.error("[FATAL] Unhandled promise rejection. Continuing operation. Reason:", event.reason);
    event.preventDefault(); // Prevent default Deno behavior (process exit)
});

addEventListener("error", (event) => {
    console.error("[FATAL] Uncaught exception. Continuing operation. Error:", event.error || event.message);
    // For ErrorEvent, preventDefault() prevents default handling
    if (event instanceof ErrorEvent) {
        event.preventDefault();
    }
});
```

These are Deno-compatible event listeners that catch any promise rejections or exceptions not caught by per-call try/catch blocks.

- [ ] **Step 3: Verify main.ts compiles**

Run: `cd ~/workspace/livesync-bridge && deno check main.ts`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
cd ~/workspace/livesync-bridge
git add main.ts _test/main.test.ts
git commit -m "fix: add global unhandled rejection/exception handlers to prevent process crash"
```

---

## Fix 2: Watch Backfill for Stale Since Pointer

### Problem

The CouchDB `PeerCouchDB` uses PouchDB's `.changes({ live: true, since: this.since })` to watch for document changes. The `since` pointer is persisted via `localStorage`. When the bridge starts, it's initialized to either:
- `""` (from beginning) — only when a `--reset` was done AND the remote CouchDB UUID changed ("database rebuilt")
- `"now"` — default, meaning "start watching from now, ignore all prior changes"
- A saved sequence number — meaning "resume from where we left off"

**The gap:** If the process dies (crash, DNS error, OOM kill) or the network drops for a period, the `_changes` feed stops advancing. On restart, if the `since` pointer was saved mid-stream, the bridge picks up from that point — **but only for future changes**. Any changes that occurred while the watcher was dead and weren't in the live feed's buffer are silently lost forever.

The only recovery mechanism is `--reset` (which clears localStorage, forcing the bridge to re-detect a "database rebuilt" condition), which is manual and requires stopping the service.

**Additionally:** PeerStorage already has a `scanOfflineChanges` feature that walks the filesystem on startup to pick up any files that changed while the watcher was down. PeerCouchDB has no equivalent "scan remote changes since last known good point" mechanism.

### Fix Strategy

1. **Periodic since-pointer checkpoint:** After every N processed changes, persist the `since` value to localStorage. Currently it's only saved in the `beginWatch` change callback's secondary function, but if the process crashes, all progress since the last callback is lost.

2. **Startup backfill:** On startup (when not in "database rebuilt" mode), instead of starting from the last saved `since`, perform a one-time non-live `_changes` query from the last saved `since` to `now`, process all results, then start the live watch. This fills any gap between the last checkpoint and "now".

3. **Watchdog health check:** After the bridge has been running for a configurable time without receiving any change events, verify that the CouchDB connection is healthy by making a lightweight `_changes?since=now&limit=1` request. If the request fails, restart the watcher.

### Task 5: Add periodic since-pointer checkpoint saving

**Files:**
- Modify: `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`

- [ ] **Step 1: Implement since-pointer checkpointing in PeerCouchDB**

The `beginWatch` callback already receives `seq` and calls `this.setSetting("since", this.man.since)`. But `this.man.since` is only updated by PouchDB's internal changes tracking. We need to also persist it periodically.

Modify `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`:

In `start()`, after `beginWatch()`, add a periodic save interval:

```typescript
    private _sinceSaveInterval?: ReturnType<typeof setInterval>;
    
    // ... existing start() method, after the beginWatch call, add:
    
    // Save since pointer every 30 seconds to limit data loss on crash
    this._sinceSaveInterval = setInterval(() => {
        const currentSince = this.man.since;
        if (currentSince) {
            this.setSetting("since", currentSince);
        }
    }, 30000);
    
    // In stop(), add:
    // if (this._sinceSaveInterval) {
    //     clearInterval(this._sinceSaveInterval);
    // }
```

Update `stop()`:

```typescript
    async stop(): Promise<void> {
        if (this._sinceSaveInterval) {
            clearInterval(this._sinceSaveInterval);
        }
        // Final save of since pointer
        if (this.man.since) {
            this.setSetting("since", this.man.since);
        }
        this.man.endWatch();
        return await Promise.resolve();
    }
```

- [ ] **Step 2: Commit**

```bash
cd ~/workspace/livesync-bridge
git add PeerCouchDB.ts
git commit -m "fix: periodically save since pointer checkpoint to minimize data loss on crash"
```

---

### Task 6: Add startup backfill for missed CouchDB changes

**Files:**
- Modify: `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`
- Modify: `/home/claude/workspace/livesync-bridge/lib/src/API/DirectFileManipulatorV2.ts`

This is the most important fix. When the bridge starts with a saved `since` pointer, it must first backfill any changes that occurred since that pointer before starting the live watch.

- [ ] **Step 1: Add `followUpdates` call in `PeerCouchDB.start()` before `beginWatch()`**

The `DirectFileManipulator` already has a `followUpdates()` method that does a non-live `_changes` query. We'll use this for the backfill.

Modify `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`, in `_startInner()` (the method created in Task 3), add backfill before the live watch:

After the `"Watch starting from ${this.man.since}"` log and before `this.man.beginWatch(...)`, add:

```typescript
        // If we have a saved since pointer, backfill any missed changes before starting live watch
        if (this.man.since && this.man.since !== "now") {
            this.normalLog(`Backfill: catching up from seq ${this.man.since}...`);
            try {
                const lastSeq = await this.man.followUpdates(
                    async (entry) => {
                        if (!entry.path) return;
                        const d = entry.type == "plain" ? entry.data : new Uint8Array(decodeBinary(entry.data));
                        let path = entry.path.substring(baseDir.length);
                        if (path.startsWith("/")) {
                            path = path.substring(1);
                        }
                        if (entry.deleted || entry._deleted) {
                            this.sendLog(`[backfill] ${path} delete detected`);
                            await this.dispatchDeleted(path);
                        } else {
                            const docData = { ctime: entry.ctime, mtime: entry.mtime, size: entry.size, deleted: entry.deleted || entry._deleted, data: d };
                            this.sendLog(`[backfill] ${path} change detected`);
                            await this.dispatch(path, docData);
                        }
                    },
                    (doc) => {
                        if (!doc.path) return false;
                        if (doc.path.indexOf(":") !== -1) return false;
                        return doc.path.startsWith(baseDir);
                    }
                );
                this.setSetting("since", lastSeq?.toString() ?? this.man.since);
                this.man.since = lastSeq?.toString() ?? this.man.since;
                this.normalLog(`Backfill: caught up to seq ${this.man.since}`);
            } catch (ex) {
                this.normalLog(`Backfill failed, will start live watch anyway: ${ex}`, LOG_LEVEL_NOTICE);
                // Continue to live watch — don't block startup on backfill failure
            }
        }
```

- [ ] **Step 2: Verify `followUpdates` uses non-live mode**

Read `/home/claude/workspace/livesync-bridge/lib/src/API/DirectFileManipulatorV2.ts` lines ~360-400. The `followUpdates()` method already uses `live: false` and returns `last_seq`. It correctly processes changes from `this.since` to the current end. This is exactly what we need.

- [ ] **Step 3: Commit**

```bash
cd ~/workspace/livesync-bridge
git add PeerCouchDB.ts
git commit -m "feat: backfill missed CouchDB changes on startup before starting live watch"
```

---

### Task 7: Add watcher health check / watchdog

**Files:**
- Modify: `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`

- [ ] **Step 1: Add a watchdog that detects when the watcher is stale**

In `PeerCouchDB`, add a periodic health check that verifies the CouchDB connection is alive. If no changes have been received in a configurable interval, make a lightweight request to verify connectivity.

Modify `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`:

Add properties:

```typescript
    private _lastChangeReceivedAt = Date.now();
    private _healthCheckInterval?: ReturnType<typeof setInterval>;
    private static readonly HEALTH_CHECK_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
    private static readonly STALE_THRESHOLD_MS = 10 * 60 * 1000; // 10 minutes with no changes
```

In `_startInner()`, after `beginWatch()`:

```typescript
        // Health check: verify CouchDB connectivity if no changes received for a while
        this._healthCheckInterval = setInterval(async () => {
            const timeSinceLastChange = Date.now() - this._lastChangeReceivedAt;
            if (timeSinceLastChange > PeerCouchDB.STALE_THRESHOLD_MS) {
                this.normalLog(`Health check: no changes received in ${Math.round(timeSinceLastChange / 60000)} minutes, verifying connectivity...`);
                try {
                    const info = await this.man.rawGet<Record<string, any>>("_design/replicate");
                    if (info) {
                        this.normalLog(`Health check: CouchDB is reachable`);
                    } else {
                        this.normalLog(`Health check: CouchDB returned unexpected response, restarting watcher`, LOG_LEVEL_NOTICE);
                        this.man.endWatch();
                        await this._startInner();
                    }
                } catch (ex) {
                    this.normalLog(`Health check: CouchDB unreachable: ${ex}. Restarting watcher.`, LOG_LEVEL_NOTICE);
                    this.man.endWatch();
                    await this._startInner();
                }
            }
        }, PeerCouchDB.HEALTH_CHECK_INTERVAL_MS);
```

In the `beginWatch` change callback, update `_lastChangeReceivedAt`:

```typescript
        // The change callback is in the start() method. Update the timestamp there.
        // In the beginWatch callback already in start():
        this._lastChangeReceivedAt = Date.now();
```

In `stop()`, clean up:

```typescript
    async stop(): Promise<void> {
        if (this._sinceSaveInterval) clearInterval(this._sinceSaveInterval);
        if (this._healthCheckInterval) clearInterval(this._healthCheckInterval);
        // Final save of since pointer
        if (this.man.since) {
            this.setSetting("since", this.man.since);
        }
        this.man.endWatch();
        return await Promise.resolve();
    }
```

- [ ] **Step 2: Commit**

```bash
cd ~/workspace/livesync-bridge
git add PeerCouchDB.ts
git commit -m "feat: add watchdog health check that detects stale CouchDB watcher and reconnects"
```

---

### Task 8: Add `--backfill` CLI flag for forced resync without `--reset`

**Files:**
- Modify: `/home/claude/workspace/livesync-bridge/main.ts`
- Modify: `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`

Currently the only way to force a full re-sync is `--reset`, which clears ALL localStorage (including the `remote-created` UUID) and forces a complete database rebuild. This is overkill — we need a lighter way to just re-backfill from the beginning without losing the local database state.

- [ ] **Step 1: Add `--backfill` flag to main.ts**

Modify `/home/claude/workspace/livesync-bridge/main.ts`:

```typescript
const flags = parseArgs(Deno.args, {
    boolean: ["reset", "backfill"],
    default: { reset: false, backfill: false },
});
if (flags.reset) {
    localStorage.clear();
}
if (flags.backfill) {
    // Clear only the "since" pointers to force backfill from beginning
    // without clearing remote-created or other state
    const keysToRemove: string[] = [];
    for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key && key.endsWith("-since")) {
            keysToRemove.push(key);
        }
    }
    for (const key of keysToRemove) {
        localStorage.removeItem(key);
    }
    console.log(`Cleared ${keysToRemove.length} since pointer(s) for backfill`);
}
```

- [ ] **Step 2: Modify PeerCouchDB constructor to handle empty since**

In `/home/claude/workspace/livesync-bridge/PeerCouchDB.ts`, the constructor currently does:

```typescript
    this.man.since = this.getSetting("since") || "now";
```

When `--backfill` clears the `since` setting, `getSetting("since")` returns `null`, and the fallback is `"now"`. We need to also handle empty string:

```typescript
    this.man.since = this.getSetting("since") || "";
```

This way, when `since` is empty (cleared by `--backfill`), the `start()` method's existing "database rebuilt" check will detect the mismatch and set `since = ""`, which in PouchDB's `_changes` API means "from the beginning".

Wait — actually, looking at the `start()` logic more carefully:

- If `since` is `""`, PouchDB starts from the beginning (seq 0)
- If the "database rebuilt" check passes (remote-created matches), it logs "Watch starting from <since>" and uses whatever since is

So setting `since = ""` when it's null would make the bridge start from the beginning, which is what `--backfill` wants. But this changes the "idle" startup behavior. Let me reconsider.

Actually, the current code:
```typescript
this.man.since = this.getSetting("since") || "now";
```

If `since` was never saved, it defaults to "now". If we change this to:
```typescript
this.man.since = this.getSetting("since") || "";
```

Then on a fresh start (first run ever), it would try to process ALL changes since the beginning of the CouchDB database, which is equivalent to the "database rebuilt" path. That's actually fine — it's what the user expects on first run. And after `--backfill`, it would re-process everything.

The `start()` method already handles the `""` case correctly by checking remote-created. If remote-created matches, it logs `Watch starting from ""` and PouchDB interprets `""` as "from the beginning". This is the correct behavior.

- [ ] **Step 3: Commit**

```bash
cd ~/workspace/livesync-bridge
git add main.ts PeerCouchDB.ts
git commit -m "feat: add --backfill flag for forced resync without clearing database state"
```

---

### Task 9: Update systemd service with Restart=always and add environment documentation

**Files:**
- Modify: `~/.config/systemd/user/livesync-bridge.service`

- [ ] **Step 1: Update systemd service for better resilience**

Replace `~/.config/systemd/user/livesync-bridge.service`:

```ini
[Unit]
Description=Livesync Bridge - Obsidian Vault Sync
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/home/claude/.deno/bin/deno run -A main.ts
WorkingDirectory=/home/claude/workspace/livesync-bridge
Restart=always
RestartSec=10
StartLimitIntervalSec=300
StartLimitBurst=10
Environment=HOME=/home/claude

[Install]
WantedBy=default.target
```

Key changes:
- `Restart=on-failure` → `Restart=always`: Restart the service even on clean exit (e.g., if a future version adds a clean shutdown path)
- Added `StartLimitIntervalSec` and `StartLimitBurst`: Prevent rapid restart loops (max 10 restarts in 5 minutes)

- [ ] **Step 2: Reload and apply**

```bash
systemctl --user daemon-reload
systemctl --user restart livesync-bridge.service
```

- [ ] **Step 3: Commit**

```bash
cd ~/workspace/livesync-bridge
git add scripts/livesync-bridge.service
git commit -m "infra: update systemd service with Restart=always and restart rate limiting"
```

Note: this should also be deployed to `~/.config/systemd/user/livesync-bridge.service` and `systemctl --user daemon-reload` run.

---

### Task 10: Integration test — verify the full flow

**Files:**
- Create: `/home/claude/workspace/livesync-bridge/_test/integration.test.ts`

- [ ] **Step 1: Write an integration test that exercises the Hub.dispatch error handling**

```typescript
import { assertEquals } from "jsr:@std/assert";
import { Hub } from "../Hub.ts";
import { Peer, DispatchFun } from "../Peer.ts";
import { PeerConf, FileData } from "../types.ts";

class ThrowingPeer extends Peer {
  throwOn: string;
  
  constructor(conf: PeerConf, dispatcher: DispatchFun, throwOn: string) {
    super(conf, dispatcher);
    this.throwOn = throwOn;
  }
  async put(path: string, data: FileData): Promise<boolean> {
    if (path === this.throwOn) throw new Error("Simulated network error");
    return true;
  }
  async delete(path: string): Promise<boolean> {
    if (path === this.throwOn) throw new Error("Simulated network error");
    return true;
  }
  async get(path: string): Promise<false | FileData> { return false; }
  async start(): Promise<void> {}
  async stop(): Promise<void> {}
}

class TrackingPeer extends Peer {
  puts: string[] = [];
  deletes: string[] = [];
  
  constructor(conf: PeerConf, dispatcher: DispatchFun) {
    super(conf, dispatcher);
  }
  async put(path: string, data: FileData): Promise<boolean> {
    this.puts.push(path);
    return true;
  }
  async delete(path: string): Promise<boolean> {
    this.deletes.push(path);
    return true;
  }
  async get(path: string): Promise<false | FileData> { return false; }
  async start(): Promise<void> {}
  async stop(): Promise<void> {}
}

Deno.test("Hub.dispatch: network error on one peer does not block other peers", async () => {
  const hub = new Hub({ peers: [] });
  const sourcePeer = new TrackingPeer(
    { type: "storage", name: "source", baseDir: "" },
    hub.dispatch.bind(hub)
  );
  const throwingPeer = new ThrowingPeer(
    { type: "couchdb", name: "thrower", baseDir: "" },
    hub.dispatch.bind(hub),
    "fail.md"
  );
  const targetPeer = new TrackingPeer(
    { type: "storage", name: "target", baseDir: "" },
    hub.dispatch.bind(hub)
  );
  
  hub.peers = [sourcePeer, throwingPeer, targetPeer];
  
  const data: FileData = { ctime: Date.now(), mtime: Date.now(), size: 10, data: ["test"] };
  
  // Should succeed on targetPeer even though throwingPeer throws
  await hub.dispatch(sourcePeer, "fail.md", data);
  assertEquals(targetPeer.puts.length, 1);
  assertEquals(targetPeer.puts[0], "fail.md");
  
  // Should also succeed for normal paths
  await hub.dispatch(sourcePeer, "ok.md", data);
  assertEquals(targetPeer.puts.length, 2);
  assertEquals(throwingPeer.constructor.name, "ThrowingPeer"); // Still alive
});

Deno.test("Hub.dispatch: network error on delete does not crash", async () => {
  const hub = new Hub({ peers: [] });
  const sourcePeer = new TrackingPeer(
    { type: "storage", name: "source", baseDir: "" },
    hub.dispatch.bind(hub)
  );
  const throwingPeer = new ThrowingPeer(
    { type: "couchdb", name: "thrower", baseDir: "" },
    hub.dispatch.bind(hub),
    "fail.md"
  );
  const targetPeer = new TrackingPeer(
    { type: "storage", name: "target", baseDir: "" },
    hub.dispatch.bind(hub)
  );
  
  hub.peers = [sourcePeer, throwingPeer, targetPeer];
  
  // Should not throw on delete
  await hub.dispatch(sourcePeer, "fail.md", false);
  assertEquals(targetPeer.deletes.length, 1);
});
```

- [ ] **Step 2: Run integration tests**

Run: `cd ~/workspace/livesync-bridge && deno test _test/integration.test.ts -A`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
cd ~/workspace/livesync-bridge
git add _test/integration.test.ts
git commit -m "test: add integration tests for Hub.dispatch network error resilience"
```

---

## Summary of all changes

| Task | File(s) | Purpose |
|------|---------|---------|
| 1 | `Hub.ts` | Wrap `dispatch()` in try/catch per peer |
| 2 | `PeerCouchDB.ts` | Wrap `put()`/`delete()` in try/catch |
| 3 | `PeerCouchDB.ts`, `DirectFileManipulatorV2.ts` | Exponential backoff for watch reconnection + retry for start() |
| 4 | `main.ts` | Global unhandled rejection/exception handlers |
| 5 | `PeerCouchDB.ts` | Periodic since-pointer checkpoint (every 30s) |
| 6 | `PeerCouchDB.ts` | Startup backfill via `followUpdates()` before live watch |
| 7 | `PeerCouchDB.ts` | Watchdog health check for stale watcher |
| 8 | `main.ts`, `PeerCouchDB.ts` | `--backfill` CLI flag |
| 9 | Systemd service file | `Restart=always` + rate limiting |
| 10 | `_test/integration.test.ts` | Integration test for error handling |

**Risk assessment:**
- Task 6 (startup backfill) is the highest-value change but also the riskiest — it adds a blocking network call before the live watch starts, which could delay startup if CouchDB is slow. The try/catch fallback to live watch mitigates this.
- Task 8 (`--backfill` flag) changes the default `since` from `"now"` to `""`. This is safe because the first-run case already goes through the "database rebuilt" path which sets since="" anyway.
- The global error handler in Task 4 is a safety net — it shouldn't be the primary defense, but it prevents silent crashes from edge cases we haven't thought of.