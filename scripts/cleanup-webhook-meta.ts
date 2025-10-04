#!/usr/bin/env bun

import Redis from "ioredis";

if (!process.env.REDIS_URL) {
  throw new Error("REDIS_URL is not set");
}

// Configuration
const CONFIG = {
  redisUrl: process.env.REDIS_URL,
  batchSize: 5000,
  dryRun: false, // Set to false to actually delete
  maxAgeHours: 3, // Delete jobs finished more than 3 hours ago
} as const;

class WebhookMetaCleanup {
  private redis: Redis;
  private stats = {
    totalScanned: 0,
    totalDeleted: 0,
    totalSkipped: 0,
    errors: 0,
    invalidTimestamps: 0,
  };

  constructor() {
    this.redis = new Redis(CONFIG.redisUrl);
  }

  async run(): Promise<void> {
    console.log(`🚀 Starting cleanup (DRY_RUN: ${CONFIG.dryRun})`);
    console.log("🎯 Target pattern:");
    console.log("   - twmq:engine-cloud_webhook:job:*:meta");
    console.log(`   - Max age: ${CONFIG.maxAgeHours} hours`);
    console.log("");

    try {
      await this.cleanOldJobMeta();
      this.printFinalStats();
    } catch (error) {
      console.error(`💥 Fatal error: ${error}`);
      throw error;
    } finally {
      await this.redis.quit();
    }
  }

  private async cleanOldJobMeta(): Promise<void> {
    const pattern = "twmq:engine-cloud_webhook:job:*:meta";
    console.log(`🔍 Scanning pattern: ${pattern}`);
    
    let cursor = "0";
    // Unix timestamps are always in UTC (seconds since Jan 1, 1970 00:00:00 UTC)
    const now = Math.floor(Date.now() / 1000);
    const cutoffTimestamp = now - (CONFIG.maxAgeHours * 60 * 60);

    console.log(`   Current time (UTC): ${now} (${new Date(now * 1000).toISOString()})`);
    console.log(`   Cutoff time (UTC): ${cutoffTimestamp} (${new Date(cutoffTimestamp * 1000).toISOString()})`);
    console.log("");

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
        this.stats.totalScanned += keys.length;
        console.log(`   Scanned ${keys.length} keys (total: ${this.stats.totalScanned})`);

        await this.processKeyBatch(keys, cutoffTimestamp);
      }
    } while (cursor !== "0");

    console.log(`✅ Scan complete: ${pattern} (scanned ${this.stats.totalScanned} keys)`);
    console.log("");
  }

  private async processKeyBatch(keys: string[], cutoffTimestamp: number): Promise<void> {
    const keysToDelete: string[] = [];

    // Batch fetch all finished_at timestamps using pipeline
    const pipeline = this.redis.pipeline();
    for (const key of keys) {
      pipeline.hget(key, "finished_at");
    }

    let results;
    try {
      results = await pipeline.exec();
    } catch (error) {
      console.error(`   💥 Error fetching timestamps batch: ${error}`);
      this.stats.errors += keys.length;
      return;
    }

    // Process results
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      if (!key) continue;
      
      const [err, finishedAt] = results?.[i] ?? [null, null];

      if (err) {
        console.error(`   💥 Error processing key ${key}: ${err}`);
        this.stats.errors += 1;
        continue;
      }

      if (!finishedAt) {
        this.stats.totalSkipped += 1;
        continue;
      }

      const finishedAtTimestamp = parseInt(finishedAt as string, 10);

      if (isNaN(finishedAtTimestamp)) {
        this.stats.invalidTimestamps += 1;
        continue;
      }

      if (finishedAtTimestamp < cutoffTimestamp) {
        const age = Math.floor((Date.now() / 1000 - finishedAtTimestamp) / 3600);
        if (keysToDelete.length < 10) {
          // Only log first 10 to avoid spam
          console.log(`   🗑️  Marking for deletion: ${key} (finished ${age}h ago)`);
        }
        keysToDelete.push(key);
      } else {
        this.stats.totalSkipped += 1;
      }
    }

    // Delete the marked keys
    if (keysToDelete.length > 0) {
      console.log(`   Found ${keysToDelete.length} keys to delete in this batch`);
      if (CONFIG.dryRun) {
        console.log(`   [DRY RUN] Would delete ${keysToDelete.length} keys`);
        this.stats.totalDeleted += keysToDelete.length;
      } else {
        await this.deleteKeys(keysToDelete);
      }
    }
  }

  private async deleteKeys(keys: string[]): Promise<void> {
    try {
      const pipeline = this.redis.pipeline();
      for (const key of keys) {
        pipeline.del(key);
      }

      const results = await pipeline.exec();
      const deletedCount = results?.filter(([err]) => err === null).length || 0;
      const failedCount = keys.length - deletedCount;

      console.log(`   ✅ Deleted ${deletedCount} keys`);
      if (failedCount > 0) {
        console.log(`   ❌ Failed to delete ${failedCount} keys`);
        this.stats.errors += failedCount;
      }

      this.stats.totalDeleted += deletedCount;
    } catch (error) {
      console.error(`   💥 Error deleting batch: ${error}`);
      this.stats.errors += keys.length;
    }
  }

  private printFinalStats(): void {
    console.log("📈 Final Statistics:");
    console.log(`   Total Scanned: ${this.stats.totalScanned.toLocaleString()}`);
    console.log(`   Total ${CONFIG.dryRun ? 'Would Delete' : 'Deleted'}: ${this.stats.totalDeleted.toLocaleString()}`);
    console.log(`   Total Skipped (not old enough): ${this.stats.totalSkipped.toLocaleString()}`);
    if (this.stats.invalidTimestamps > 0) {
      console.log(`   Invalid Timestamps: ${this.stats.invalidTimestamps.toLocaleString()}`);
    }
    if (this.stats.errors > 0) {
      console.log(`   Errors: ${this.stats.errors.toLocaleString()}`);
    }
    console.log("");
    
    if (CONFIG.dryRun) {
      console.log("💡 This was a DRY RUN - no data was actually deleted");
      console.log("💡 Set CONFIG.dryRun = false to actually delete the keys");
    } else {
      console.log("✅ CLEANUP COMPLETED - Data has been permanently deleted");
    }
  }
}

// Main execution
async function main() {
  const cleaner = new WebhookMetaCleanup();
  await cleaner.run();
}

if (import.meta.main) {
  main().catch(console.error);
}

