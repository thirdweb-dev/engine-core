# Start local chain with anvil + speedbump proxy (300ms variable latency)
local-chain:
    #!/usr/bin/env bash
    set -e
    
    echo "🧹 Cleaning up existing processes..."
    # Kill any existing anvil or speedbump processes
    pkill -f "anvil.*8546" || true
    pkill -f "speedbump.*8545" || true
    lsof -ti:8545 | xargs kill -9 2>/dev/null || true
    lsof -ti:8546 | xargs kill -9 2>/dev/null || true
    
    echo "🔨 Starting anvil on port 8546 (1s blocktime)..."
    anvil --port 8546 --block-time 1 &
    ANVIL_PID=$!
    
    # Cleanup function
    cleanup() {
        echo ""
        echo "🛑 Stopping services..."
        kill $ANVIL_PID 2>/dev/null || true
        pkill -f "speedbump.*8545" || true
        exit 0
    }
    
    trap cleanup INT TERM EXIT
    
    # Wait a moment for anvil to start
    sleep 2
    
    echo "🐌 Starting speedbump proxy on port 8545 (→ localhost:8546)"
    echo "   Latency: 300ms base + 150ms sine wave (150-450ms variable)"
    echo "   Connect to: http://localhost:8545"
    echo ""
    speedbump --port=8545 --latency=300ms --sine-amplitude=150ms --sine-period=1m localhost:8546

# Fund an address with 1 ETH (bypasses speedbump for faster setup)
fund address:
    @echo "💰 Funding {{address}} with 1 ETH..."
    @cast rpc anvil_setBalance {{address}} $(cast to-wei 1) --rpc-url http://localhost:8546
    @echo "✅ Done!"

