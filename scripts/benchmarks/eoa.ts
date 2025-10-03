/// <reference lib="dom" />

// Bun globals (runtime will provide these)
declare const Bun: any;
declare const process: any;

// Extend ImportMeta for Bun
interface ImportMeta {
  dir: string;
}

// Types based on events.rs
interface WebhookEvent {
    transactionId: string;
    executorName: string;
  stageName: string; // "send" | "confirm"
  eventType: string; // "Success" | "Nack" | "Failure"
  payload: any;
}

interface TransactionMetrics {
  transactionId: string;
  httpResponseTime: number;
  sentTime: number;
  submittedTime?: number;
  confirmedTime?: number;
  sentToSubmittedMs?: number;
  submittedToConfirmedMs?: number;
  totalTimeMs?: number;
  status: "pending" | "submitted" | "confirmed" | "failed";
  error?: string;
}

interface BenchmarkConfig {
  baseUrl: string;
  type: string;
  from: string;
  chainId: number;
  secretKey: string;
  vaultAccessToken: string;
  concurrentRequests: number;
  totalRequests: number;
}

interface AggregateResults {
  totalRequests: number;
  successfulRequests: number;
  failedRequests: number;
  errorRate: number;
  httpResponseTimes: {
    min: number;
    max: number;
    mean: number;
    p50: number;
    p90: number;
    p95: number;
    p99: number;
  };
  sentToSubmittedTimes: {
    min: number;
    max: number;
    mean: number;
    p50: number;
    p90: number;
    p95: number;
    p99: number;
  };
  submittedToConfirmedTimes: {
    min: number;
    max: number;
    mean: number;
    p50: number;
    p90: number;
    p95: number;
    p99: number;
  };
  totalTimes: {
    min: number;
    max: number;
    mean: number;
    p50: number;
    p90: number;
    p95: number;
    p99: number;
  };
  duration: number;
  throughput: number;
}

// Global state to track transactions
const transactions = new Map<string, TransactionMetrics>();
const pendingTransactions = new Set<string>();

// Configuration
const config: BenchmarkConfig = {
  baseUrl: process.env.BASE_URL || "http://localhost:3005",
  type: process.env.TYPE || "eoa",
  from: process.env.FROM!,
  chainId: parseInt(process.env.CHAIN_ID || "1337"),
  secretKey: process.env.SECRET_KEY!,
  vaultAccessToken: process.env.VAULT_ACCESS_TOKEN!,
  concurrentRequests: parseInt(process.env.CONCURRENT_REQUESTS || "10"),
  totalRequests: parseInt(process.env.TOTAL_REQUESTS || "100"),
};

// Validate required env vars
if (!config.from || !config.secretKey || !config.vaultAccessToken) {
  console.error("❌ Missing required environment variables: FROM, SECRET_KEY, VAULT_ACCESS_TOKEN");
  process.exit(1);
}

// Setup webhook server
const webhookServer = Bun.serve({
  port: 3070,
  async fetch(req: Request) {
    if (req.method === "POST" && new URL(req.url).pathname === "/callback") {
      try {
        const event: WebhookEvent = await req.json();
        handleWebhookEvent(event);
        return new Response("OK", { status: 200 });
      } catch (error) {
        console.error("Error handling webhook:", error);
        return new Response("Error", { status: 500 });
      }
    }
    return new Response("Not Found", { status: 404 });
  },
});

console.log(`🎣 Webhook server listening on http://localhost:${webhookServer.port}`);

// Handle webhook events
function handleWebhookEvent(event: WebhookEvent) {
  const txId = event.transactionId;
  const metrics = transactions.get(txId);

  if (!metrics) {
    console.warn(`⚠️  Received webhook for unknown transaction: ${txId}`);
    return;
  }

  const now = Date.now();

  // Handle different stage events
  if (event.stageName === "send") {
    if (event.eventType === "SUCCESS") {
      metrics.submittedTime = now;
      metrics.sentToSubmittedMs = now - metrics.sentTime;
      metrics.status = "submitted";
      console.log(`✅ Transaction ${txId.slice(0, 8)}... submitted (${metrics.sentToSubmittedMs}ms)`);
    } else if (event.eventType === "Failure") {
      metrics.status = "failed";
      metrics.error = JSON.stringify(event.payload);
      pendingTransactions.delete(txId);
      console.log(`❌ Transaction ${txId.slice(0, 8)}... failed at send stage (pending: ${pendingTransactions.size})`);
    }
  } else if (event.stageName === "confirm") {
    if (event.eventType === "SUCCESS") {
      metrics.confirmedTime = now;
      if (metrics.submittedTime) {
        metrics.submittedToConfirmedMs = now - metrics.submittedTime;
      }
      metrics.totalTimeMs = now - metrics.sentTime;
      metrics.status = "confirmed";
      pendingTransactions.delete(txId);
      console.log(`🎉 Transaction ${txId.slice(0, 8)}... confirmed (total: ${metrics.totalTimeMs}ms, pending: ${pendingTransactions.size})`);
    } else if (event.eventType === "FAIL" || event.eventType === "NACK") {
      metrics.status = "failed";
      metrics.error = JSON.stringify(event.payload);
      pendingTransactions.delete(txId);
      console.log(`❌ Transaction ${txId.slice(0, 8)}... failed at confirmation stage (pending: ${pendingTransactions.size})`);
    }
  }
}

// Send a single transaction
async function sendTransaction(): Promise<TransactionMetrics> {
  const startTime = performance.now();

  try {
    const response = await fetch(`${config.baseUrl}/v1/write/transaction`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-thirdweb-secret-key": config.secretKey,
        "x-vault-access-token": config.vaultAccessToken,
      },
      body: JSON.stringify({
        executionOptions: {
          type: config.type,
          from: config.from,
          chainId: config.chainId,
        },
        params: [
          {
            to: "0x2247d5d238d0f9d37184d8332aE0289d1aD9991b",
            data: "0x",
            value: "0",
          },
        ],
        webhookOptions: [
          {
            url: "http://localhost:3070/callback",
          },
        ],
      }),
    });

    const endTime = performance.now();
    const httpResponseTime = endTime - startTime;

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`HTTP ${response.status}: ${errorText}`);
    }

    const res = await response.json() as {
        result: {
            transactions: {
                id: string;
            }[]
        }
    };
    const transactionId = res.result.transactions[0]?.id;

    if (!transactionId) {
      console.error("❌ No transaction ID in response:", JSON.stringify(response, null, 2));
      throw new Error("No transaction ID in response");
    }

    // Set sentTime AFTER we get the HTTP response, so we measure from when
    // the server accepted the request to when webhooks fire
    const sentTime = Date.now();

    const metrics: TransactionMetrics = {
      transactionId,
      httpResponseTime,
      sentTime,
      status: "pending",
    };

    transactions.set(transactionId, metrics);
    pendingTransactions.add(transactionId);
    
    console.log(`📝 Added transaction ${transactionId.slice(0, 8)}... to pending (total: ${pendingTransactions.size})`);

    return metrics;
  } catch (error) {
    const httpResponseTime = performance.now() - startTime;
    const sentTime = Date.now();
    const errorMetrics: TransactionMetrics = {
      transactionId: `error-${Date.now()}`,
      httpResponseTime,
      sentTime,
      status: "failed",
      error: error instanceof Error ? error.message : String(error),
    };
    transactions.set(errorMetrics.transactionId, errorMetrics);
    console.error(`❌ Transaction request failed: ${error instanceof Error ? error.message : String(error)}`);
    return errorMetrics;
  }
}

// Run benchmark with concurrency control
async function runBenchmark() {
  console.log("\n🚀 Starting benchmark...");
  console.log(`📊 Configuration:`);
  console.log(`   Base URL: ${config.baseUrl}`);
  console.log(`   Type: ${config.type}`);
  console.log(`   From: ${config.from}`);
  console.log(`   Chain ID: ${config.chainId}`);
  console.log(`   Total Requests: ${config.totalRequests}`);
  console.log(`   Concurrent Requests: ${config.concurrentRequests}`);
  console.log();

  const startTime = Date.now();
  const allPromises: Promise<TransactionMetrics>[] = [];
  const inFlight = new Set<Promise<TransactionMetrics>>();

  // Send requests with concurrency control using sliding window
  for (let i = 0; i < config.totalRequests; i++) {
    const promise = sendTransaction();
    allPromises.push(promise);
    inFlight.add(promise);

    // Remove from in-flight set when completed
    promise.finally(() => inFlight.delete(promise));

    // Control concurrency - wait if we've reached the limit
    if (inFlight.size >= config.concurrentRequests) {
      await Promise.race(inFlight);
    }

    // Progress indicator
    if ((i + 1) % 10 === 0) {
      console.log(`📤 Sent ${i + 1}/${config.totalRequests} requests... (in-flight: ${inFlight.size})`);
    }
  }

  // Wait for all HTTP requests to complete
  await Promise.all(allPromises);
  console.log(`\n✅ All HTTP requests completed`);
  console.log(`   Pending transactions: ${pendingTransactions.size}`);

  // Wait for all webhooks to be received (with timeout)
  console.log(`⏳ Waiting for webhooks to complete...`);
  const maxWaitTime = 600000; // 5 minutes
  const pollInterval = 1000; // 1 second
  let waited = 0;

  while (pendingTransactions.size > 0 && waited < maxWaitTime) {
    await new Promise((resolve) => setTimeout(resolve, pollInterval));
    waited += pollInterval;

    if (waited % 5000 === 0) {
      console.log(`   Still waiting for ${pendingTransactions.size} transactions...`);
    }
  }

  const endTime = Date.now();
  const duration = endTime - startTime;

  if (pendingTransactions.size > 0) {
    console.warn(`\n⚠️  Timeout: ${pendingTransactions.size} transactions still pending`);
  } else {
    console.log(`\n🎉 All transactions completed!`);
  }

  return { startTime, endTime, duration };
}

// Calculate statistics
function calculateStats(values: number[]): {
  min: number;
  max: number;
  mean: number;
  p50: number;
  p90: number;
  p95: number;
  p99: number;
} {
  if (values.length === 0) {
    return { min: 0, max: 0, mean: 0, p50: 0, p90: 0, p95: 0, p99: 0 };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, val) => acc + val, 0);

  return {
    min: sorted[0] ?? 0,
    max: sorted[sorted.length - 1] ?? 0,
    mean: sum / sorted.length,
    p50: sorted[Math.floor(sorted.length * 0.5)] ?? 0,
    p90: sorted[Math.floor(sorted.length * 0.9)] ?? 0,
    p95: sorted[Math.floor(sorted.length * 0.95)] ?? 0,
    p99: sorted[Math.floor(sorted.length * 0.99)] ?? 0,
  };
}

// Generate aggregate results
function generateAggregateResults(duration: number): AggregateResults {
  const allMetrics = Array.from(transactions.values());
  const successfulMetrics = allMetrics.filter((m) => m.status === "confirmed");
  const failedMetrics = allMetrics.filter((m) => m.status === "failed");

  const httpResponseTimes = allMetrics.map((m) => m.httpResponseTime);
  const sentToSubmittedTimes = successfulMetrics
    .filter((m) => m.sentToSubmittedMs !== undefined)
    .map((m) => m.sentToSubmittedMs!);
  const submittedToConfirmedTimes = successfulMetrics
    .filter((m) => m.submittedToConfirmedMs !== undefined)
    .map((m) => m.submittedToConfirmedMs!);
  const totalTimes = successfulMetrics
    .filter((m) => m.totalTimeMs !== undefined)
    .map((m) => m.totalTimeMs!);

  return {
    totalRequests: allMetrics.length,
    successfulRequests: successfulMetrics.length,
    failedRequests: failedMetrics.length,
    errorRate: (failedMetrics.length / allMetrics.length) * 100,
    httpResponseTimes: calculateStats(httpResponseTimes),
    sentToSubmittedTimes: calculateStats(sentToSubmittedTimes),
    submittedToConfirmedTimes: calculateStats(submittedToConfirmedTimes),
    totalTimes: calculateStats(totalTimes),
    duration,
    throughput: (allMetrics.length / duration) * 1000, // requests per second
  };
}

// Write CSV file
async function writeCSV(outputDir: string, timestamp: string) {
  const csvPath = `${outputDir}/transactions-${timestamp}.csv`;
  const headers = [
    "transaction_id",
    "http_response_time_ms",
    "sent_to_submitted_ms",
    "submitted_to_confirmed_ms",
    "total_time_ms",
    "status",
    "error",
  ];

  const rows = Array.from(transactions.values()).map((m) => [
    m.transactionId,
    m.httpResponseTime.toFixed(2),
    m.sentToSubmittedMs?.toFixed(2) || "",
    m.submittedToConfirmedMs?.toFixed(2) || "",
    m.totalTimeMs?.toFixed(2) || "",
    m.status,
    m.error || "",
  ]);

  const csvContent = [headers.join(","), ...rows.map((row) => row.join(","))].join("\n");

  await Bun.write(csvPath, csvContent);
  console.log(`📄 CSV written to: ${csvPath}`);
}

// Write JSON results
async function writeJSON(outputDir: string, timestamp: string, results: AggregateResults) {
  const jsonPath = `${outputDir}/result-${timestamp}.json`;
  await Bun.write(jsonPath, JSON.stringify(results, null, 2));
  console.log(`📊 Results written to: ${jsonPath}`);
}

// Print results to console
function printResults(results: AggregateResults) {
  console.log("\n" + "=".repeat(60));
  console.log("📊 BENCHMARK RESULTS");
  console.log("=".repeat(60));

  console.log("\n📈 Overview:");
  console.log(`   Total Requests:      ${results.totalRequests}`);
  console.log(`   Successful:          ${results.successfulRequests}`);
  console.log(`   Failed:              ${results.failedRequests}`);
  console.log(`   Error Rate:          ${results.errorRate.toFixed(2)}%`);
  console.log(`   Duration:            ${(results.duration / 1000).toFixed(2)}s`);
  console.log(`   Throughput:          ${results.throughput.toFixed(2)} req/s`);

  console.log("\n⏱️  HTTP Response Times (ms):");
  console.log(`   Min:    ${results.httpResponseTimes.min.toFixed(2)}`);
  console.log(`   Mean:   ${results.httpResponseTimes.mean.toFixed(2)}`);
  console.log(`   P50:    ${results.httpResponseTimes.p50.toFixed(2)}`);
  console.log(`   P90:    ${results.httpResponseTimes.p90.toFixed(2)}`);
  console.log(`   P95:    ${results.httpResponseTimes.p95.toFixed(2)}`);
  console.log(`   P99:    ${results.httpResponseTimes.p99.toFixed(2)}`);
  console.log(`   Max:    ${results.httpResponseTimes.max.toFixed(2)}`);

  console.log("\n📤 Sent to Submitted Times (ms):");
  console.log(`   Min:    ${results.sentToSubmittedTimes.min.toFixed(2)}`);
  console.log(`   Mean:   ${results.sentToSubmittedTimes.mean.toFixed(2)}`);
  console.log(`   P50:    ${results.sentToSubmittedTimes.p50.toFixed(2)}`);
  console.log(`   P90:    ${results.sentToSubmittedTimes.p90.toFixed(2)}`);
  console.log(`   P95:    ${results.sentToSubmittedTimes.p95.toFixed(2)}`);
  console.log(`   P99:    ${results.sentToSubmittedTimes.p99.toFixed(2)}`);
  console.log(`   Max:    ${results.sentToSubmittedTimes.max.toFixed(2)}`);

  console.log("\n✅ Submitted to Confirmed Times (ms):");
  console.log(`   Min:    ${results.submittedToConfirmedTimes.min.toFixed(2)}`);
  console.log(`   Mean:   ${results.submittedToConfirmedTimes.mean.toFixed(2)}`);
  console.log(`   P50:    ${results.submittedToConfirmedTimes.p50.toFixed(2)}`);
  console.log(`   P90:    ${results.submittedToConfirmedTimes.p90.toFixed(2)}`);
  console.log(`   P95:    ${results.submittedToConfirmedTimes.p95.toFixed(2)}`);
  console.log(`   P99:    ${results.submittedToConfirmedTimes.p99.toFixed(2)}`);
  console.log(`   Max:    ${results.submittedToConfirmedTimes.max.toFixed(2)}`);

  console.log("\n🎯 Total Times (Sent to Confirmed) (ms):");
  console.log(`   Min:    ${results.totalTimes.min.toFixed(2)}`);
  console.log(`   Mean:   ${results.totalTimes.mean.toFixed(2)}`);
  console.log(`   P50:    ${results.totalTimes.p50.toFixed(2)}`);
  console.log(`   P90:    ${results.totalTimes.p90.toFixed(2)}`);
  console.log(`   P95:    ${results.totalTimes.p95.toFixed(2)}`);
  console.log(`   P99:    ${results.totalTimes.p99.toFixed(2)}`);
  console.log(`   Max:    ${results.totalTimes.max.toFixed(2)}`);

  console.log("\n" + "=".repeat(60));
}

// Main execution
async function main() {
  try {
    // Run benchmark
    const { duration } = await runBenchmark();

    // Generate results
    const results = generateAggregateResults(duration);

    // Create output directory
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const currentDir = (import.meta as any).dir;
    const outputDir = `${currentDir}/runs/run-${timestamp}`;
    await Bun.$`mkdir -p ${outputDir}`.quiet();

    // Write outputs
    await writeCSV(outputDir, timestamp);
    await writeJSON(outputDir, timestamp, results);

    // Print results
    printResults(results);

    console.log(`\n✨ Benchmark complete! Results saved to: ${outputDir}\n`);
  } catch (error) {
    console.error("❌ Benchmark failed:", error);
    webhookServer.stop();
    process.exit(1);
  }

  // Stop webhook server and exit
  webhookServer.stop();
  process.exit(0);
}

// Run the benchmark
main();

