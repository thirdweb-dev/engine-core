#!/usr/bin/env bun

// Configuration
const HOST = "https://engine-core-rs-prod-c620.engine-aws-usw2.zeet.app";
const PASSWORD = "donut7-staging-tumbling-planner-stage-eclair";
const EOA = "0xE0F28D9d95143858Be492BDf3abBCA746d0d2272";
const CHAIN_ID = 1628;
const EOA_CHAIN_ID = `${EOA}:${CHAIN_ID}`;

const PENDING_COUNT_THRESHOLD = 50;
const NONCE_STALE_THRESHOLD_MS = 30 * 1000; // 30 seconds
const POLL_INTERVAL_MS = 10 * 1000; // 10 seconds

interface HealthData {
  balance: string;
  balance_threshold: string;
  balance_fetched_at: number;
  last_confirmation_at: number;
  last_nonce_movement_at: number;
  nonce_resets: number[];
}

interface EOAState {
  eoa: string;
  chainId: number;
  cachedNonce: number;
  optimisticNonce: number;
  pendingCount: number;
  submittedCount: number;
  borrowedCount: number;
  recycledNoncesCount: number;
  recycledNonces: number[];
  health: HealthData;
  manualResetScheduled: boolean;
}

interface APIResponse {
  result: EOAState;
}

// State tracking
let lastCachedNonce: number | null = null;
let lastNonceChangeTime: number = Date.now();

const headers = {
  "x-diagnostic-access-password": PASSWORD,
  "Content-Type": "application/json",
};

async function fetchEOAState(): Promise<EOAState | null> {
  try {
    const url = `${HOST}/admin/executors/eoa/${EOA_CHAIN_ID}/state`;
    console.log(
      `🔍 [${new Date().toLocaleString()}] Fetching EOA state from: ${url}`
    );

    const response = await fetch(url, { headers });

    if (!response.ok) {
      console.error(
        `❌ [${new Date().toLocaleString()}] Failed to fetch EOA state: ${
          response.status
        } ${response.statusText}`
      );
      return null;
    }

    const data: APIResponse = await response.json();
    return data.result;
  } catch (error) {
    console.error(
      `❌ [${new Date().toLocaleString()}] Error fetching EOA state:`,
      error
    );
    return null;
  }
}

async function resetEOA(): Promise<boolean> {
  try {
    const url = `${HOST}/admin/executors/eoa/${EOA_CHAIN_ID}/reset`;
    console.log(
      `🔄 [${new Date().toLocaleString()}] Triggering EOA reset at: ${url}`
    );

    const response = await fetch(url, {
      method: "POST",
      headers,
    });

    if (!response.ok) {
      console.error(
        `❌ [${new Date().toLocaleString()}] Failed to reset EOA: ${
          response.status
        } ${response.statusText}`
      );
      return false;
    }

    console.log(
      `✅ [${new Date().toLocaleString()}] EOA reset triggered successfully`
    );
    return true;
  } catch (error) {
    console.error(
      `❌ [${new Date().toLocaleString()}] Error resetting EOA:`,
      error
    );
    return false;
  }
}

function shouldTriggerReset(state: EOAState): boolean {
  const now = Date.now();
  // Track nonce changes
  if (lastCachedNonce === null) {
    lastCachedNonce = state.cachedNonce;
    lastNonceChangeTime = now;
    return false;
  }

  // If nonce changed, update tracking
  if (state.cachedNonce !== lastCachedNonce) {
    lastCachedNonce = state.cachedNonce;
    lastNonceChangeTime = now;
    return false;
  }

  // Check if pending count is above threshold
  if (state.pendingCount <= PENDING_COUNT_THRESHOLD) {
    return false;
  }

  // Check if nonce has been stale for too long
  const nonceStaleDuration = now - lastNonceChangeTime;

  return nonceStaleDuration >= NONCE_STALE_THRESHOLD_MS;
}

function logState(state: EOAState) {
  const now = new Date();
  const isoTime = now.toISOString();
  const localeTime = now.toLocaleString();
  const nonceStaleDuration = Date.now() - lastNonceChangeTime;

  console.log(`\n📊 [${localeTime}] (${isoTime}) EOA State Summary:`);
  console.log(`   EOA: ${state.eoa}`);
  console.log(`   Chain ID: ${state.chainId}`);
  console.log(`   Cached Nonce: ${state.cachedNonce}`);
  console.log(`   Optimistic Nonce: ${state.optimisticNonce}`);
  console.log(
    `   Pending Count: ${state.pendingCount} ${
      state.pendingCount > PENDING_COUNT_THRESHOLD ? "⚠️  HIGH" : "✅"
    }`
  );
  console.log(`   Submitted Count: ${state.submittedCount}`);
  console.log(
    `   Nonce Stale Duration: ${Math.round(nonceStaleDuration / 1000)}s ${
      nonceStaleDuration >= NONCE_STALE_THRESHOLD_MS ? "⚠️  STALE" : "✅"
    }`
  );
  console.log(`   Balance: ${parseInt(state.health.balance, 16)} wei`);
  console.log(
    `   Manual Reset Scheduled: ${
      state.manualResetScheduled ? "⚠️  YES" : "✅ NO"
    }`
  );
}

async function monitorLoop() {
  console.log("🚀 Starting EOA Monitor");
  console.log(`📍 Monitoring: ${EOA_CHAIN_ID}`);
  console.log(`🔄 Poll interval: ${POLL_INTERVAL_MS / 1000}s`);
  console.log(
    `⚠️  Reset conditions: pendingCount > ${PENDING_COUNT_THRESHOLD} AND nonce stale > ${
      NONCE_STALE_THRESHOLD_MS / 1000
    }s`
  );
  console.log("=".repeat(80));

  while (true) {
    const state = await fetchEOAState();

    if (state) {
      logState(state);

      if (shouldTriggerReset(state)) {
        console.log(
          `\n🚨 [${new Date().toLocaleString()}] RESET CONDITIONS MET!`
        );
        console.log(
          `   - Pending count (${state.pendingCount}) > ${PENDING_COUNT_THRESHOLD}`
        );
        console.log(
          `   - Nonce stale for ${Math.round(
            (Date.now() - lastNonceChangeTime) / 1000
          )}s > ${NONCE_STALE_THRESHOLD_MS / 1000}s`
        );

        const resetSuccess = await resetEOA();
        if (resetSuccess) {
          // Reset tracking after successful reset
          lastCachedNonce = null;
          lastNonceChangeTime = Date.now();
        }
      }
    } else {
      console.log(
        `❌ [${new Date().toLocaleString()}] Failed to fetch EOA state, skipping this iteration`
      );
    }

    console.log(
      `\n⏱️  Waiting ${POLL_INTERVAL_MS / 1000}s until next check...`
    );
    console.log("-".repeat(80));

    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
  }
}

// Start monitoring
await monitorLoop();
