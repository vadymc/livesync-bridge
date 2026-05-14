import { DirectFileManipulator, FileInfo, MetaEntry, ReadyEntry } from "./lib/src/API/DirectFileManipulatorV2.ts";
import { DocumentID, FilePathWithPrefix, LOG_LEVEL_NOTICE, MILESTONE_DOCID, TweakValues } from "./lib/src/common/types.ts";
import { PeerCouchDBConf, FileData } from "./types.ts";
import { decodeBinary } from "./lib/src/string_and_binary/convert.ts";
import { isPlainText } from "./lib/src/string_and_binary/path.ts";
import { DispatchFun, Peer } from "./Peer.ts";
import { createBinaryBlob, createTextBlob, isDocContentSame, unique } from "./lib/src/common/utils.ts";

// export class PeerInstance()

export class PeerCouchDB extends Peer {
    man: DirectFileManipulator;
    declare config: PeerCouchDBConf;
    private _sinceSaveInterval?: ReturnType<typeof setInterval>;
    private _healthCheckInterval?: ReturnType<typeof setInterval>;
    private _lastChangeReceivedAt = Date.now();
    private static readonly HEALTH_CHECK_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
    private static readonly STALE_THRESHOLD_MS = 10 * 60 * 1000; // 10 minutes with no changes
    constructor(conf: PeerCouchDBConf, dispatcher: DispatchFun) {
        super(conf, dispatcher);
        this.man = new DirectFileManipulator(conf);
        // Fetch remote since.
        this.man.since = this.getSetting("since") || "now";
    }
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
            // const old = await this.getMeta(path as FilePathWithPrefix);
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
    async get(pathSrc: FilePathWithPrefix): Promise<false | FileData> {
        const path = this.toLocalPath(pathSrc) as FilePathWithPrefix;
        const ret = await this.man.get(path) as false | ReadyEntry;
        if (ret === false) {
            return false;
        }
        return {
            ctime: ret.ctime,
            mtime: ret.mtime,
            data: ret.type == "newnote" ? new Uint8Array(decodeBinary(ret.data)) : ret.data,
            size: ret.size,
            deleted: ret.deleted
        };
    }
    async getMeta(pathSrc: FilePathWithPrefix): Promise<false | FileData> {
        const path = this.toLocalPath(pathSrc) as FilePathWithPrefix;
        const ret = await this.man.get(path, true) as false | MetaEntry;
        if (ret === false) {
            return false;
        }
        return {
            ctime: ret.ctime,
            mtime: ret.mtime,
            data: [],
            size: ret.size,
            deleted: ret.deleted
        };
    }
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
        const baseDir = this.toLocalPath("");
        await this.man.ready.promise;
        const w = await this.man.rawGet<Record<string, any>>(MILESTONE_DOCID);
        if (w && "tweak_values" in w) {
            if (this.config.useRemoteTweaks) {
                const tweaks = Object.values(w["tweak_values"])[0] as TweakValues;
                // console.log(tweaks)
                const orgConf = { ...this.config } as Record<string, any>;
                this.config.customChunkSize = tweaks.customChunkSize ?? this.config.customChunkSize;
                this.config.minimumChunkSize = tweaks.minimumChunkSize ?? this.config.minimumChunkSize;
                if (tweaks.encrypt && !this.config.passphrase) {
                    throw new Error("Remote database is encrypted but no passphrase provided.");
                }
                if (tweaks.usePathObfuscation && !this.config.obfuscatePassphrase) {
                    throw new Error("Remote database is obfuscated but no obfuscate passphrase provided.");
                }
                this.config.hashAlg = tweaks.hashAlg ?? this.config.hashAlg;
                this.config.maxAgeInEden = tweaks.maxAgeInEden ?? this.config.maxAgeInEden;
                this.config.maxTotalLengthInEden = tweaks.maxTotalLengthInEden ?? this.config.maxTotalLengthInEden;
                this.config.maxChunksInEden = tweaks.maxChunksInEden ?? this.config.maxChunksInEden;
                this.config.useEden = tweaks.useEden ?? this.config.useEden;
                if (!this.config.enableCompression != !tweaks.enableCompression) {
                    throw new Error("Compression setting mismatched.");
                }
                this.config.useDynamicIterationCount = tweaks.useDynamicIterationCount ?? this.config.useDynamicIterationCount;
                this.config.enableChunkSplitterV2 = tweaks.enableChunkSplitterV2 ?? this.config.enableChunkSplitterV2;
                this.config.chunkSplitterVersion = tweaks.chunkSplitterVersion ?? this.config.chunkSplitterVersion;
                this.config.E2EEAlgorithm = tweaks.E2EEAlgorithm ?? this.config.E2EEAlgorithm;
                this.config.minimumChunkSize = tweaks.minimumChunkSize ?? this.config.minimumChunkSize;
                this.config.customChunkSize = tweaks.customChunkSize ?? this.config.customChunkSize;
                this.config.doNotUseFixedRevisionForChunks = tweaks.doNotUseFixedRevisionForChunks ?? this.config.doNotUseFixedRevisionForChunks;
                this.config.handleFilenameCaseSensitive = tweaks.handleFilenameCaseSensitive ?? this.config.handleFilenameCaseSensitive;
                const newConf = { ...this.config } as Record<string, any>;
                this.man.options = this.config;
                await this.man.liveSyncLocalDB.initializeDatabase()
                // await this.man.managers.initManagers();
                const diff = unique([...Object.keys(orgConf), ...Object.keys(tweaks)]).filter(k => orgConf[k] != newConf[k]);
                if (diff.length > 0) {
                    this.normalLog(`Remote tweaks changed --->`);
                    for (const diffKey of diff) {
                        this.normalLog(`${diffKey}\t: ${orgConf[diffKey]} \t : ${newConf[diffKey]}`);
                    }
                    this.normalLog(`<--- Remote tweaks changed`);
                }
            }
        }
        if (!w) {
            this.normalLog(`Remote database looks like empty. fetch from the first.`);
            this.setSetting("remote-created", "0");
            return;
        }
        const created = w.created;
        if (this.getSetting("remote-created") !== `${created}`) {
            this.man.since = "";
            this.normalLog(`Remote database looks like rebuilt. fetch from the first again.`);
            this.setSetting("remote-created", `${created}`);
        } else {
            this.normalLog(`Watch starting from ${this.man.since}`);
        }

        // Force backfill requested via --backfill flag: start from the beginning
        if (this.getSetting("force-backfill") === "true") {
            this.man.since = "0";
            this.setSetting("force-backfill", ""); // Clear flag
            this.normalLog(`Force backfill requested, starting from beginning`);
        }

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

        this.man.beginWatch(async (entry) => {
            this._lastChangeReceivedAt = Date.now();
            if (!entry.path) return;
            const d = entry.type == "plain" ? entry.data : new Uint8Array(decodeBinary(entry.data));
            let path = entry.path.substring(baseDir.length);
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            if (entry.deleted || entry._deleted) {
                this.sendLog(`${path} delete detected`);
                await this.dispatchDeleted(path);
            } else {
                const docData = { ctime: entry.ctime, mtime: entry.mtime, size: entry.size, deleted: entry.deleted || entry._deleted, data: d };
                this.sendLog(`${path} change detected`);
                await this.dispatch(path, docData);
            }
        }, (entry) => {
            this.setSetting("since", this.man.since);
            if (!entry.path) return false;
            if (entry.path.indexOf(":") !== -1) return false;
            return entry.path.startsWith(baseDir);
        });

        // Periodic since pointer save — every 30 seconds to limit data loss on crash
        this._sinceSaveInterval = setInterval(() => {
            const currentSince = this.man.since;
            if (currentSince) {
                this.setSetting("since", currentSince);
            }
        }, 30000);

        // Health check: verify CouchDB connectivity if no changes received for a while
        this._healthCheckInterval = setInterval(async () => {
            const timeSinceLastChange = Date.now() - this._lastChangeReceivedAt;
            if (timeSinceLastChange > PeerCouchDB.STALE_THRESHOLD_MS) {
                this.normalLog(`Health check: no changes received in ${Math.round(timeSinceLastChange / 60000)} minutes, verifying connectivity...`);
                try {
                    const info = await this.man.rawGet<Record<string, any>>("" as DocumentID);
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
    }
    async dispatch(path: string, data: FileData | false) {
        if (data === false) return;
        if (!await this.isRepeating(path, data)) {
            await this.dispatchToHub(this, this.toGlobalPath(path), data);
        }
        // else {
        //     this.receiveLog(`${path} dispatch repeating`);
        // }
    }
    async dispatchDeleted(path: string) {
        if (!await this.isRepeating(path, false)) {
            await this.dispatchToHub(this, this.toGlobalPath(path), false);
        }
    }
    async stop(): Promise<void> {
        if (this._sinceSaveInterval) {
            clearInterval(this._sinceSaveInterval);
            this._sinceSaveInterval = undefined;
        }
        if (this._healthCheckInterval) {
            clearInterval(this._healthCheckInterval);
            this._healthCheckInterval = undefined;
        }
        // Final save of since pointer
        if (this.man.since) {
            this.setSetting("since", this.man.since);
        }
        this.man.endWatch();
        return await Promise.resolve();
    }
}
