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

import { defaultLoggerEnv } from "./lib/src/common/logger.ts";
import { LOG_LEVEL_DEBUG } from "./lib/src/common/logger.ts";
import { Hub } from "./Hub.ts";
import { Config } from "./types.ts";
import { parseArgs } from "jsr:@std/cli";

const KEY = "LSB_"
defaultLoggerEnv.minLogLevel = LOG_LEVEL_DEBUG;
const configFile = Deno.env.get(`${KEY}CONFIG`) || "./dat/config.json";

console.log("LiveSync Bridge is now starting...");
let config: Config = { peers: [] };
const flags = parseArgs(Deno.args, {
    boolean: ["reset", "backfill"],
    // string: ["version"],
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
    // Set flag for PeerCouchDB to pick up
    localStorage.setItem("force-backfill", "true");
    console.log(`Cleared ${keysToRemove.length} since pointer(s) for backfill`);
}
try {
    const confText = await Deno.readTextFile(configFile);
    config = JSON.parse(confText);
} catch (ex) {
    console.error("Could not parse configuration!");
    console.error(ex);
}
console.log("LiveSync Bridge is now started!");
const hub = new Hub(config);
hub.start();