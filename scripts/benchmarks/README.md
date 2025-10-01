# EOA Executor Benchmark

A comprehensive benchmark tool for stress testing the `/v1/write/transaction` endpoint with detailed performance metrics and webhook event tracking.

## Features

- ✅ Concurrent request execution with configurable concurrency
- 📊 Detailed metrics tracking (HTTP response time, submission time, confirmation time)
- 🎣 Built-in webhook server to capture transaction lifecycle events
- 📈 Statistical analysis (p50, p90, p95, p99, min, max, mean)
- 📄 CSV export of individual transaction metrics
- 📊 JSON export of aggregate results
- 🎯 Real-time progress and status updates

## Setup

1. Copy the `.env.example` to `.env` in the `/scripts` directory:
   ```bash
   cp .env.example .env
   ```

2. Configure your environment variables in `.env`:
   ```
   BASE_URL=http://localhost:3005
   TYPE=eoa
   FROM=0x1234567890123456789012345678901234567890
   CHAIN_ID=1337
   SECRET_KEY=your-secret-key
   VAULT_ACCESS_TOKEN=your-vault-access-token
   CONCURRENT_REQUESTS=10
   TOTAL_REQUESTS=100
   ```

## Usage

Run the benchmark from the `/scripts` directory:

```bash
cd scripts
bun ./benchmarks/eoa.ts
```

## Output

The benchmark creates a timestamped directory `run-<timestamp>/` containing:

- `transactions-<timestamp>.csv` - Individual transaction metrics
- `result-<timestamp>.json` - Aggregate statistics

### CSV Format

Each row contains:
- `transaction_id` - Unique transaction identifier
- `http_response_time_ms` - HTTP request/response time
- `sent_to_submitted_ms` - Time from send to submitted webhook
- `submitted_to_confirmed_ms` - Time from submitted to confirmed webhook
- `total_time_ms` - Total time from send to confirmed
- `status` - Final status (confirmed/failed/pending)
- `error` - Error message if failed

### JSON Format

Aggregate results include:
- Total/successful/failed request counts
- Error rate percentage
- Duration and throughput (req/s)
- Statistical breakdown for all timing metrics (min, max, mean, p50, p90, p95, p99)

## Configuration Options

| Variable | Default | Description |
|----------|---------|-------------|
| `BASE_URL` | `http://localhost:3005` | API endpoint base URL |
| `TYPE` | `eoa` | Execution type |
| `FROM` | *required* | Sender address |
| `CHAIN_ID` | `1337` | Blockchain network ID |
| `SECRET_KEY` | *required* | API secret key |
| `VAULT_ACCESS_TOKEN` | *required* | Vault access token |
| `CONCURRENT_REQUESTS` | `10` | Number of concurrent requests |
| `TOTAL_REQUESTS` | `100` | Total number of requests to send |

## How It Works

1. **Webhook Server**: Starts a local server on port 3070 to receive transaction lifecycle webhooks
2. **Request Sending**: Sends HTTP requests to `/v1/write/transaction` with controlled concurrency
3. **Event Tracking**: Tracks webhook events for each transaction:
   - `send` stage with `Success` event = transaction submitted
   - `confirm` stage with `Success` event = transaction confirmed
   - `Failure` or `Nack` events = transaction failed
4. **Metrics Calculation**: Computes timing metrics and statistics
5. **Output Generation**: Writes CSV and JSON files with results

## Webhook Event Structure

Based on `executors/src/eoa/events.rs`, the webhook payload contains:
```json
{
  "transaction_id": "...",
  "executor_name": "eoa",
  "stage_name": "send" | "confirm",
  "event_type": "Success" | "Nack" | "Failure",
  "payload": { ... }
}
```

## Example Output

```
🚀 Starting benchmark...
📊 Configuration:
   Base URL: http://localhost:3005
   Type: eoa
   From: 0x1234...
   Chain ID: 1337
   Total Requests: 100
   Concurrent Requests: 10

📤 Sent 10/100 requests...
...
✅ All HTTP requests completed
⏳ Waiting for webhooks to complete...
🎉 All transactions completed!

📊 BENCHMARK RESULTS
============================================================
📈 Overview:
   Total Requests:      100
   Successful:          98
   Failed:              2
   Error Rate:          2.00%
   Duration:            45.23s
   Throughput:          2.21 req/s

⏱️  HTTP Response Times (ms):
   Min:    45.23
   Mean:   123.45
   P50:    110.00
   P90:    180.00
   P95:    210.00
   P99:    250.00
   Max:    320.00
...
```

## Tips

- Start with a small `TOTAL_REQUESTS` value to test your setup
- Adjust `CONCURRENT_REQUESTS` based on your server capacity
- Monitor your server logs alongside the benchmark
- The script waits up to 2 minutes for pending webhooks before timing out
- Use the CSV output for detailed per-transaction analysis
- Use the JSON output for automated performance regression testing


