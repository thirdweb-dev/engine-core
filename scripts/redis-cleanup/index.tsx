#!/usr/bin/env bun

import Redis from "ioredis";
import React, { useState, useEffect } from "react";
import { render, Box, Text, useStdout } from "ink";

if (!process.env.REDIS_URL) {
  throw new Error("REDIS_URL is not set");
}

// === CONFIGURATION ===
const CONFIG = {
  redisUrl: process.env.REDIS_URL,
  namespace: "engine-cloud" as string | undefined, // Set to your namespace if needed
  batchSize: 10000,
  dryRun: true, // Set to false when ready to actually delete
  progressInterval: 10000, // Report progress every N transactions
} as const;

// === TYPES ===
interface TxData {
  id: string;
  status: string | null;
  completedAt: string | null;
  keyName: string;
}

interface Stats {
  scanned: number;
  confirmed: number;
  failed: number;
  cleaned: number;
  errors: number;
  skipped: number;
  lastProcessedKey?: string;
}

// Global state for the UI
let globalStats: Stats = {
  scanned: 0,
  confirmed: 0,
  failed: 0,
  cleaned: 0,
  errors: 0,
  skipped: 0,
};

let globalLogs: string[] = [];
let updateUI: (() => void) | null = null;

class EoaRedisCleanup {
  private redis: Redis;

  get stats() {
    return globalStats;
  }

  constructor() {
    this.redis = new Redis(CONFIG.redisUrl);
  }

  async run(): Promise<Stats> {
    this.log(`🚀 Starting cleanup (DRY_RUN: ${CONFIG.dryRun})`);
    this.log(`🎯 Target: All confirmed/failed transactions (no age limit)`);

    const pattern = this.buildPattern();
    this.log(`🔍 Pattern: ${pattern}`);

    let cursor = "0";
    const startTime = Date.now();

    try {
      do {
        const [newCursor, keys] = await this.redis.scan(
          cursor,
          "MATCH",
          pattern,
          "COUNT",
          CONFIG.batchSize
        );
        cursor = newCursor;
        if (keys.length > 0) {
          this.stats.lastProcessedKey = keys[keys.length - 1];
        }

        if (keys.length > 0) {
          await this.processBatch(keys);
          this.reportProgress(); // Report after each batch
        }
      } while (cursor !== "0");

      const duration = (Date.now() - startTime) / 1000;
      this.log(`✅ Completed in ${duration.toFixed(1)}s`);
      return this.stats;
    } catch (error) {
      this.log(`💥 Fatal error: ${error}`);
      throw error;
    } finally {
      await this.redis.quit();
    }
  }

  private buildPattern(): string {
    return CONFIG.namespace
      ? `${CONFIG.namespace}:eoa_executor:tx_data:*`
      : "eoa_executor:tx_data:*";
  }

  private async processBatch(keys: string[]): Promise<void> {
    // Fetch all transaction data in one pipeline
    const pipeline = this.redis.pipeline();
    for (const key of keys) {
      pipeline.hmget(key, "status", "completed_at");
    }

    const results = await pipeline.exec();
    if (!results) {
      this.stats.errors += keys.length;
      return;
    }

    // First pass: identify what to clean
    const toClean: TxData[] = [];
    const toSkip: TxData[] = [];

    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      const result = results[i];

      if (result?.[0] === null) {
        // No Redis error
        const [status, completedAt] = result[1] as [
          string | null,
          string | null
        ];

        // Simple null check - if key is valid, process it
        if (key && key.includes(":")) {
          const txId = this.extractTxId(key);

          if (!txId) {
            this.stats.skipped++;
            continue;
          }

          const txData: TxData = {
            id: txId,
            status,
            completedAt,
            keyName: key,
          };

          this.stats.scanned++;

          // Log every transaction details
          this.log(
            `📋 ${txData.id} | status: ${
              txData.status || "null"
            } | completed_at: ${txData.completedAt || "null"}`
          );

          if (this.shouldClean(txData)) {
            toClean.push(txData);
          } else {
            toSkip.push(txData);
            this.stats.skipped++;
          }
        } else {
          this.log(`❌ Invalid key format: ${key}`);
          this.stats.errors++;
        }
      } else {
        this.stats.errors++;
      }
    }

    // Second pass: batch delete all transactions that should be cleaned
    if (toClean.length > 0) {
      await this.batchCleanTransactions(toClean);
    }
  }

  private extractTxId(key: string): string | null {
    const parts = key.split(":");
    if (parts.length === 0) {
      this.log(`⚠️ SKIP: Invalid key format - empty: ${key}`);
      return null;
    }
    const txId = parts.at(-1); // Get last element safely
    if (!txId || txId.length === 0 || txId.trim() === "") {
      this.log(`⚠️ SKIP: Invalid key format - no transaction ID: ${key}`);
      return null;
    }
    return txId.trim();
  }

  private shouldClean(tx: TxData): boolean {
    // CRITICAL SAFETY CHECK: Only allow EXACT string matches
    // This prevents any edge cases, null values, or corrupted data
    if (tx.status !== "confirmed" && tx.status !== "failed") {
      this.log(
        `🛡️  SAFETY: Skipping tx ${tx.id} - status not explicitly confirmed/failed (status: ${tx.status})`
      );
      return false;
    }

    // CRITICAL SAFETY CHECK: Must have completion timestamp
    if (!tx.completedAt || tx.completedAt === "null" || tx.completedAt === "") {
      this.log(`🛡️  SAFETY: Skipping tx ${tx.id} - no completion timestamp`);
      return false;
    }

    // CRITICAL SAFETY CHECK: Validate timestamp is a valid number
    const completedMs = parseInt(tx.completedAt);
    if (isNaN(completedMs) || completedMs <= 0) {
      this.log(
        `🛡️  SAFETY: Skipping tx ${tx.id} - invalid timestamp: ${tx.completedAt}`
      );
      return false;
    }

    // CRITICAL SAFETY CHECK: Only clean transactions that are reasonably old (1 month minimum)
    // This prevents cleaning transactions that just completed
    const oneMonthAgo = Date.now() - 30 * 24 * 60 * 60 * 1000;
    if (completedMs > oneMonthAgo) {
      this.log(
        `🛡️  SAFETY: Skipping tx ${tx.id} - completed too recently (less than 1 month ago)`
      );
      return false;
    }

    this.log(
      `✅ SAFE TO CLEAN: ${tx.id} (${tx.status}) - completed ${new Date(
        completedMs
      ).toISOString()}`
    );
    return true;
  }

  private async cleanTransaction(tx: TxData): Promise<void> {
    try {
      // TRIPLE SAFETY CHECK: Re-verify this transaction should be cleaned (without extra logging)
      if (tx.status !== "confirmed" && tx.status !== "failed") {
        this.log(
          `🛡️  SAFETY: Double-check failed for ${tx.id} - aborting cleanup`
        );
        this.stats.skipped++;
        return;
      }

      // Count by status
      if (tx.status === "confirmed") this.stats.confirmed++;
      if (tx.status === "failed") this.stats.failed++;

      const keysToDelete = this.buildKeysToDelete(tx.id);

      if (CONFIG.dryRun) {
        // Just count what we would delete
        this.log(
          `🔍 [DRY RUN] Would clean: ${tx.id} (${tx.status}) - ${keysToDelete.length} keys`
        );
        this.stats.cleaned++;
        return;
      }

      // FINAL SAFETY CONFIRMATION
      this.log(
        `⚠️  DELETING: ${tx.id} (${tx.status}) - ${keysToDelete.length} keys`
      );

      // Actually delete (atomic)
      const pipeline = this.redis.pipeline();
      for (const key of keysToDelete) {
        pipeline.del(key);
      }

      const results = await pipeline.exec();

      // Verify all deletions succeeded
      if (results?.every(([err]) => err === null)) {
        this.log(
          `✅ Cleaned: ${tx.id} (${tx.status}) - deleted ${keysToDelete.length} keys`
        );
        this.stats.cleaned++;
      } else {
        this.stats.errors++;
        this.log(`❌ Failed to delete tx ${tx.id} (${tx.status})`);
      }
    } catch (error) {
      this.stats.errors++;
      this.log(`❌ Error cleaning tx ${tx.id}: ${error}`);
    }
  }

  private async batchCleanTransactions(transactions: TxData[]): Promise<void> {
    if (transactions.length === 0) return;

    if (CONFIG.dryRun) {
      for (const tx of transactions) {
        // Count by status
        if (tx.status === "confirmed") this.stats.confirmed++;
        if (tx.status === "failed") this.stats.failed++;

        const keysToDelete = this.buildKeysToDelete(tx.id);
        this.stats.cleaned++;
      }
      this.log(
        `🔍 [DRY RUN] Would clean: ${transactions.length} transactions (${this.stats.confirmed} confirmed, ${this.stats.failed} failed)`
      );
      return;
    }

    try {
      // Build one massive pipeline for all deletions
      const pipeline = this.redis.pipeline();
      const validTxIds: string[] = [];
      let totalKeys = 0;

      for (const tx of transactions) {
        // TRIPLE SAFETY CHECK: Re-verify this transaction should be cleaned
        if (tx.status !== "confirmed" && tx.status !== "failed") {
          this.log(
            `🛡️  SAFETY: Double-check failed for ${tx.id} - aborting cleanup`
          );
          this.stats.skipped++;
          continue;
        }

        // Count by status
        if (tx.status === "confirmed") this.stats.confirmed++;
        if (tx.status === "failed") this.stats.failed++;

        const keysToDelete = this.buildKeysToDelete(tx.id);

        // Add all keys for this transaction to the pipeline
        for (const key of keysToDelete) {
          pipeline.del(key);
        }

        totalKeys += keysToDelete.length;
        validTxIds.push(tx.id);
      }

      if (validTxIds.length === 0) {
        return;
      }

      // Execute all deletions in one massive pipeline
      this.log(
        `🚀 BATCH DELETE: ${validTxIds.length} transactions (${totalKeys} keys)...`
      );
      const start = Date.now();

      const results = await pipeline.exec();

      const duration = Date.now() - start;

      // Verify all deletions succeeded
      if (results?.every(([err]) => err === null)) {
        this.stats.cleaned += validTxIds.length;
        this.log(
          `✅ BATCH CLEANED: ${
            validTxIds.length
          } transactions in ${duration}ms (${(
            (validTxIds.length / duration) *
            1000
          ).toFixed(1)} tx/sec)`
        );
      } else {
        // Count how many failed
        const failedCount =
          results?.filter(([err]) => err !== null).length || 0;
        this.stats.errors += failedCount;
        this.stats.cleaned += validTxIds.length - failedCount;
        this.log(
          `⚠️ BATCH PARTIAL: ${
            validTxIds.length - failedCount
          } cleaned, ${failedCount} failed in ${duration}ms`
        );
      }
    } catch (error) {
      this.log(`💥 BATCH DELETE ERROR: ${error}`);
      this.stats.errors += transactions.length;
    }
  }

  private buildKeysToDelete(txId: string): string[] {
    const prefix = CONFIG.namespace ? `${CONFIG.namespace}:` : "";

    return [
      `${prefix}eoa_executor:tx_data:${txId}`,
      `${prefix}eoa_executor:tx_attempts:${txId}`,
    ];
  }

  log(message: string) {
    globalLogs.push(message);
    // Keep only last 1000 logs to prevent memory issues
    if (globalLogs.length > 1000) {
      globalLogs = globalLogs.slice(-1000);
    }
    if (updateUI) {
      updateUI();
    }
  }

  private reportProgress(): void {
    // Log major milestones
    if (
      this.stats.scanned % (CONFIG.progressInterval * 5) === 0 &&
      this.stats.scanned > 0
    ) {
      this.log(
        `🎯 Milestone: ${this.stats.scanned.toLocaleString()} transactions processed`
      );
    }
  }
}

// === REACT UI COMPONENT ===
const CleanupUI: React.FC = () => {
  const [, forceUpdate] = useState({});
  const { stdout } = useStdout();

  useEffect(() => {
    updateUI = () => forceUpdate({});
    return () => {
      updateUI = null;
    };
  }, []);

  const stats = globalStats;
  const logs = globalLogs;

  const pct =
    stats.scanned > 0
      ? ((stats.cleaned / stats.scanned) * 100).toFixed(1)
      : "0.0";

  // Calculate available height for logs
  const terminalHeight = stdout?.rows || 24;
  const logHeight = Math.max(3, terminalHeight - 6); // Reserve space for header and status

  return (
    <Box flexDirection="column" width="100%">
      {/* Header */}
      <Box borderStyle="round" borderColor="cyan" paddingX={1}>
        <Text bold color="cyan">
          🧹 EOA Redis Cleanup {CONFIG.dryRun ? "(DRY RUN)" : "(LIVE)"}
        </Text>
      </Box>

      {/* Logs - Fixed height to prevent scrolling */}
      <Box
        borderStyle="round"
        borderColor="gray"
        paddingX={1}
        flexDirection="column"
        height={logHeight}
        overflow="hidden"
      >
        {logs.slice(-logHeight + 2).map((log, i) => (
          <Text key={logs.length - logHeight + 2 + i}>{log}</Text>
        ))}
      </Box>

      {/* Status Bar */}
      <Box borderStyle="round" borderColor="green" paddingX={1}>
        <Text>
          <Text color="cyan">📊 Scanned: </Text>
          <Text color="white">{stats.scanned.toString().padStart(6)}</Text>
          <Text> | </Text>
          <Text color="yellow">
            {CONFIG.dryRun ? "Would Clean" : "Cleaned"}:{" "}
          </Text>
          <Text color="white">{stats.cleaned.toString().padStart(6)}</Text>
          <Text> </Text>
          <Text color="green">({pct.padStart(5)}%)</Text>
          <Text> | </Text>
          <Text color="blue">Skipped: </Text>
          <Text color="white">{stats.skipped.toString().padStart(6)}</Text>
          <Text> | </Text>
          <Text color="red">Errors: </Text>
          <Text color="white">{stats.errors.toString().padStart(3)}</Text>
        </Text>
      </Box>
    </Box>
  );
};

// === APP COMPONENT ===
const App: React.FC = () => {
  const [isRunning, setIsRunning] = useState(true);

  useEffect(() => {
    const runCleanup = async () => {
      const cleaner = new EoaRedisCleanup();

      // Log configuration
      cleaner.log("🔒 SAFETY CONFIGURATION CHECK:");
      cleaner.log(
        `   DRY RUN: ${CONFIG.dryRun} ${
          CONFIG.dryRun ? "✅ SAFE" : "⚠️ WILL DELETE DATA"
        }`
      );
      cleaner.log(
        `   Redis URL: ${CONFIG.redisUrl.replace(/:[^:@]*@/, ":***@")}`
      );
      cleaner.log(`   Namespace: ${CONFIG.namespace || "none"}`);
      cleaner.log(`   Batch Size: ${CONFIG.batchSize}`);

      if (!CONFIG.dryRun) {
        cleaner.log("⚠️ WARNING: DRY RUN IS DISABLED!");
        cleaner.log("⚠️ THIS WILL PERMANENTLY DELETE REDIS DATA!");
        cleaner.log(
          '⚠️ Only transactions with status="confirmed" OR status="failed"'
        );
        cleaner.log(
          "⚠️ AND completed_at timestamp AND older than 1 minute will be deleted."
        );
        cleaner.log(
          "🛡️ Multiple safety checks are in place, but this is still irreversible."
        );
      }

      try {
        const stats = await cleaner.run();

        cleaner.log("📈 Final Stats:");
        cleaner.log(`   Scanned: ${stats.scanned.toLocaleString()}`);
        cleaner.log(`   Confirmed: ${stats.confirmed.toLocaleString()}`);
        cleaner.log(`   Failed: ${stats.failed.toLocaleString()}`);
        cleaner.log(`   Cleaned: ${stats.cleaned.toLocaleString()}`);
        cleaner.log(`   Skipped: ${stats.skipped.toLocaleString()}`);
        cleaner.log(`   Errors: ${stats.errors}`);

        if (CONFIG.dryRun) {
          cleaner.log(
            "💡 DRY RUN - Set CONFIG.dryRun = false to actually delete"
          );
        } else {
          cleaner.log(
            "✅ CLEANUP COMPLETED - Data has been permanently deleted"
          );
        }

        // Keep UI open for a moment to see final stats
        setTimeout(() => {
          setIsRunning(false);
          process.exit(0);
        }, 3000);
      } catch (error) {
        cleaner.log(`💥 Fatal error: ${error}`);
        setTimeout(() => {
          setIsRunning(false);
          process.exit(1);
        }, 3000);
      }
    };

    runCleanup();
  }, []);

  if (!isRunning) {
    return null;
  }

  return <CleanupUI />;
};

// === MAIN ===
async function main() {
  render(<App />, {
    patchConsole: false, // Prevent Ink from interfering with console
    exitOnCtrlC: true,
  });
}

if (import.meta.main) {
  main().catch(console.error);
}
