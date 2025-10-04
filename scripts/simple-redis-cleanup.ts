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
} as const;

class SimpleRedisCleanup {
  private redis: Redis;
  private stats = {
    useropErrors: 0,
    useropResults: 0,
    eip7702Errors: 0,
    eip7702Results: 0,
    webhookErrors: 0,
    webhookResults: 0,
    externalBundlerErrors: 0,
    externalBundlerResults: 0,
    totalDeleted: 0,
    errors: 0,
  };

  constructor() {
    this.redis = new Redis(CONFIG.redisUrl);
  }

  async run(): Promise<void> {
    console.log(`🚀 Starting cleanup (DRY_RUN: ${CONFIG.dryRun})`);
    console.log("🎯 Target patterns:");
    console.log("   - twmq:engine-cloud_userop_confirm:job:*:errors");
    console.log("   - twmq:engine-cloud_userop_confirm:jobs:result (hash)");
    console.log("   - twmq:engine-cloud_eip7702_send:job:*:errors");
    console.log("   - twmq:engine-cloud_eip7702_send:jobs:result (hash)");
    console.log("   - twmq:engine-cloud_webhook:job:*:errors");
    console.log("   - twmq:engine-cloud_webhook:jobs:result (hash)");
    console.log("   - twmq:engine-cloud_external_bundler_send:job:*:errors");
    console.log("   - twmq:engine-cloud_external_bundler_send:jobs:result (hash)");
    console.log("");

    try {
      // Clean userop confirm keys
      await this.cleanPattern("twmq:engine-cloud_userop_confirm:job:*:errors");
      await this.cleanHash("twmq:engine-cloud_userop_confirm:jobs:result", "userop_confirm");
      
      // Clean eip7702 send keys
      await this.cleanPattern("twmq:engine-cloud_eip7702_send:job:*:errors");
      await this.cleanHash("twmq:engine-cloud_eip7702_send:jobs:result", "eip7702_send");
      
      // Clean webhook keys
      await this.cleanPattern("twmq:engine-cloud_webhook:job:*:errors");
      await this.cleanHash("twmq:engine-cloud_webhook:jobs:result", "webhook");
      
      // Clean external bundler send keys
      await this.cleanPattern("twmq:engine-cloud_external_bundler_send:job:*:errors");
      await this.cleanHash("twmq:engine-cloud_external_bundler_send:jobs:result", "external_bundler_send");

      this.printFinalStats();
    } catch (error) {
      console.error(`💥 Fatal error: ${error}`);
      throw error;
    } finally {
      await this.redis.quit();
    }
  }

  private async cleanPattern(pattern: string): Promise<void> {
    console.log(`🔍 Scanning pattern: ${pattern}`);
    
    let cursor = "0";
    let totalFound = 0;

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
        totalFound += keys.length;
        console.log(`   Found ${keys.length} keys (total: ${totalFound})`);

        if (CONFIG.dryRun) {
          console.log(`   [DRY RUN] Would delete ${keys.length} keys`);
          this.updateStats(pattern, keys.length);
        } else {
          await this.deleteKeys(keys);
          this.updateStats(pattern, keys.length);
        }
      }
    } while (cursor !== "0");

    console.log(`✅ Pattern complete: ${pattern} (found ${totalFound} keys)`);
    console.log("");
  }

  private async cleanHash(key: string, queueType: string): Promise<void> {
    console.log(`🔍 Checking hash: ${key}`);
    
    try {
      const exists = await this.redis.exists(key);
      
      if (exists) {
        const fieldCount = await this.redis.hlen(key);
        console.log(`   Found hash with ${fieldCount} fields`);

        if (CONFIG.dryRun) {
          console.log(`   [DRY RUN] Would delete hash with ${fieldCount} fields`);
          this.updateStatsForHash(queueType, fieldCount);
        } else {
          await this.redis.del(key);
          console.log(`   ✅ Deleted hash with ${fieldCount} fields`);
          this.updateStatsForHash(queueType, fieldCount);
          this.stats.totalDeleted += 1;
        }
      } else {
        console.log(`   Hash does not exist`);
      }
      
      console.log(`✅ Hash complete: ${key}`);
      console.log("");
    } catch (error) {
      console.error(`   💥 Error handling hash: ${error}`);
      this.stats.errors += 1;
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

  private updateStats(pattern: string, count: number): void {
    if (pattern.includes("userop_confirm")) {
      this.stats.useropErrors += count;
    } else if (pattern.includes("eip7702_send")) {
      this.stats.eip7702Errors += count;
    } else if (pattern.includes("webhook")) {
      this.stats.webhookErrors += count;
    } else if (pattern.includes("external_bundler_send")) {
      this.stats.externalBundlerErrors += count;
    }
  }

  private updateStatsForHash(queueType: string, count: number): void {
    if (queueType === "userop_confirm") {
      this.stats.useropResults += count;
    } else if (queueType === "eip7702_send") {
      this.stats.eip7702Results += count;
    } else if (queueType === "webhook") {
      this.stats.webhookResults += count;
    } else if (queueType === "external_bundler_send") {
      this.stats.externalBundlerResults += count;
    }
  }

  private printFinalStats(): void {
    console.log("📈 Final Statistics:");
    console.log(`   Userop Confirm:`);
    console.log(`     - Errors: ${this.stats.useropErrors.toLocaleString()}`);
    console.log(`     - Result Hash Fields: ${this.stats.useropResults.toLocaleString()}`);
    console.log(`   EIP-7702 Send:`);
    console.log(`     - Errors: ${this.stats.eip7702Errors.toLocaleString()}`);
    console.log(`     - Result Hash Fields: ${this.stats.eip7702Results.toLocaleString()}`);
    console.log(`   Webhook:`);
    console.log(`     - Errors: ${this.stats.webhookErrors.toLocaleString()}`);
    console.log(`     - Result Hash Fields: ${this.stats.webhookResults.toLocaleString()}`);
    console.log(`   External Bundler Send:`);
    console.log(`     - Errors: ${this.stats.externalBundlerErrors.toLocaleString()}`);
    console.log(`     - Result Hash Fields: ${this.stats.externalBundlerResults.toLocaleString()}`);
    console.log(`   Total ${CONFIG.dryRun ? 'Would Delete' : 'Deleted'}: ${this.stats.totalDeleted.toLocaleString()}`);
    if (this.stats.errors > 0) {
      console.log(`   Errors: ${this.stats.errors}`);
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
  const cleaner = new SimpleRedisCleanup();
  await cleaner.run();
}

if (import.meta.main) {
  main().catch(console.error);
}
