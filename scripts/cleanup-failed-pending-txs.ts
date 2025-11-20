#!/usr/bin/env bun

import Redis from "ioredis";

if (!process.env.REDIS_URL) {
  throw new Error("REDIS_URL is not set");
}

// Configuration
const CONFIG = {
  redisUrl: process.env.REDIS_URL,
  chainId: 8453,
  walletAddress: "0x80c08de1a05Df2bD633CF520754e40fdE3C794d3",
  fetchBatchSize: 1000, // batch size for fetching pending txs from sorted set
  deleteBatchSize: 100, // batch size for deletions
  dryRun: true, // Set to false to actually delete
} as const;

class FailedPendingTxCleanup {
  private redis: Redis;
  private pendingTxsKey: string;
  private stats = {
    totalPendingTxs: 0,
    totalChecked: 0,
    failedTxsFound: 0,
    deleted: 0,
    errors: 0,
  };

  constructor() {
    this.redis = new Redis(CONFIG.redisUrl);
    this.pendingTxsKey = `engine-cloud:eoa_executor:pending_txs:${CONFIG.chainId}:${CONFIG.walletAddress}`;
  }

  async run(): Promise<void> {
    console.log(`🚀 Starting failed pending transaction cleanup (DRY_RUN: ${CONFIG.dryRun})`);
    console.log(`🎯 Target:`);
    console.log(`   Chain ID: ${CONFIG.chainId}`);
    console.log(`   Wallet: ${CONFIG.walletAddress}`);
    console.log(`   Pending txs key: ${this.pendingTxsKey}`);
    console.log(`   Fetch batch size: ${CONFIG.fetchBatchSize}`);
    console.log(`   Delete batch size: ${CONFIG.deleteBatchSize}`);
    console.log("");

    try {
      // Get total count of pending transactions
      const totalCount = await this.redis.zcard(this.pendingTxsKey);
      this.stats.totalPendingTxs = totalCount;
      
      if (totalCount === 0) {
        console.log("✅ No pending transactions found");
        this.printFinalStats();
        return;
      }

      console.log(`📊 Found ${totalCount.toLocaleString()} pending transactions`);
      console.log("");

      // Process in batches
      await this.processPendingTxsInBatches();
      
      this.printFinalStats();
    } catch (error) {
      console.error(`💥 Fatal error: ${error}`);
      throw error;
    } finally {
      await this.redis.quit();
    }
  }

  private async processPendingTxsInBatches(): Promise<void> {
    let offset = 0;
    let batchNumber = 1;
    const failedTxIds: string[] = [];

    while (true) {
      console.log(`🔍 Processing batch ${batchNumber} (offset: ${offset})`);
      
      // Fetch batch of transaction IDs from sorted set (sorted by score ascending)
      const txIds = await this.redis.zrange(
        this.pendingTxsKey,
        offset,
        offset + CONFIG.fetchBatchSize - 1
      );

      if (txIds.length === 0) {
        break;
      }

      console.log(`   Retrieved ${txIds.length} transaction IDs`);

      // Check status for each transaction in parallel using pipeline
      const statuses = await this.checkTransactionStatuses(txIds);
      
      // Collect failed transaction IDs
      for (let i = 0; i < txIds.length; i++) {
        const txId = txIds[i];
        const status = statuses[i];
        console.log(`   txId: ${txId}, status: ${status}`);
        
        this.stats.totalChecked++;
        
        if (status === "failed" && txId) {
          failedTxIds.push(txId);
          this.stats.failedTxsFound++;
        }
      }

      console.log(`   Found ${statuses.filter(s => s === "failed").length} failed transactions in this batch`);

      // Delete failed transactions if we've accumulated enough
      if (failedTxIds.length >= CONFIG.deleteBatchSize) {
        await this.deleteFailedTxsInBatches(failedTxIds);
        failedTxIds.length = 0; // Clear the array
      }

      offset += CONFIG.fetchBatchSize;
      batchNumber++;
      console.log("");
    }

    // Delete any remaining failed transactions
    if (failedTxIds.length > 0) {
      await this.deleteFailedTxsInBatches(failedTxIds);
    }
  }

  private async checkTransactionStatuses(txIds: string[]): Promise<(string | null)[]> {
    try {
      const pipeline = this.redis.pipeline();
      
      for (const txId of txIds) {
        const txDataKey = `engine-cloud:eoa_executor:tx_data:${txId}`;
        pipeline.hget(txDataKey, "status");
      }

      const results = await pipeline.exec();
      
      if (!results) {
        console.error(`   ⚠️  Pipeline returned null results`);
        this.stats.errors += txIds.length;
        return new Array(txIds.length).fill(null);
      }

      return results.map(([err, status], index) => {
        if (err) {
          console.error(`   ⚠️  Error checking status for tx ${txIds[index]}: ${err}`);
          this.stats.errors++;
          return null;
        }
        return status as string | null;
      });
    } catch (error) {
      console.error(`   💥 Error in batch status check: ${error}`);
      this.stats.errors += txIds.length;
      return new Array(txIds.length).fill(null);
    }
  }

  private async deleteFailedTxsInBatches(failedTxIds: string[]): Promise<void> {
    let offset = 0;
    
    while (offset < failedTxIds.length) {
      const batch = failedTxIds.slice(offset, offset + CONFIG.deleteBatchSize);
      
      if (CONFIG.dryRun) {
        console.log(`   [DRY RUN] Would delete ${batch.length} failed transactions from pending set`);
        console.log(`   [DRY RUN] Sample IDs: ${batch.slice(0, 3).join(", ")}${batch.length > 3 ? "..." : ""}`);
        this.stats.deleted += batch.length;
      } else {
        await this.deleteFailedTxs(batch);
      }
      
      offset += CONFIG.deleteBatchSize;
    }
  }

  private async deleteFailedTxs(txIds: string[]): Promise<void> {
    try {
      // Use ZREM to remove multiple members from the sorted set at once
      const deletedCount = await this.redis.zrem(this.pendingTxsKey, ...txIds);
      
      console.log(`   ✅ Deleted ${deletedCount} failed transactions from pending set`);
      
      if (deletedCount < txIds.length) {
        const notFound = txIds.length - deletedCount;
        console.log(`   ⚠️  ${notFound} transactions were not found in the set (may have been already removed)`);
      }
      
      this.stats.deleted += deletedCount;
    } catch (error) {
      console.error(`   💥 Error deleting batch: ${error}`);
      this.stats.errors += txIds.length;
    }
  }

  private printFinalStats(): void {
    console.log("📈 Final Statistics:");
    console.log(`   Total Pending Transactions: ${this.stats.totalPendingTxs.toLocaleString()}`);
    console.log(`   Total Checked: ${this.stats.totalChecked.toLocaleString()}`);
    console.log(`   Failed Transactions Found: ${this.stats.failedTxsFound.toLocaleString()}`);
    console.log(`   ${CONFIG.dryRun ? 'Would Delete' : 'Deleted'}: ${this.stats.deleted.toLocaleString()}`);
    if (this.stats.errors > 0) {
      console.log(`   Errors: ${this.stats.errors}`);
    }
    console.log("");
    
    if (CONFIG.dryRun) {
      console.log("💡 This was a DRY RUN - no data was actually deleted");
      console.log("💡 Set CONFIG.dryRun = false to actually delete the failed transactions");
    } else {
      console.log("✅ CLEANUP COMPLETED - Failed transactions have been permanently removed from pending set");
    }
  }
}

// Main execution
async function main() {
  const cleaner = new FailedPendingTxCleanup();
  await cleaner.run();
}

if (import.meta.main) {
  main().catch(console.error);
}

