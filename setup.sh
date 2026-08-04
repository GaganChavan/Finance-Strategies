#!/bin/bash

set -e

echo "========================================"
echo "  ETF Portfolio Tracker — Setup Script  "
echo "========================================"

# ── 1. Check Python 3 is installed ───────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    echo "❌ Python 3 is not installed."
    echo "   Install it from https://www.python.org/downloads/ and re-run this script."
    exit 1
fi

echo "✅ Python found: $(python3 --version)"

# ── 2. Create a virtual environment ──────────────────────────────────────────
if [ ! -d "venv" ]; then
    echo ""
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created."
else
    echo "✅ Virtual environment already exists — skipping creation."
fi

# ── 3. Activate virtual environment ──────────────────────────────────────────
source venv/bin/activate
echo "✅ Virtual environment activated."

# ── 4. Upgrade pip ───────────────────────────────────────────────────────────
echo ""
echo "Upgrading pip..."
pip install --upgrade pip --quiet

# ── 5. Install dependencies ───────────────────────────────────────────────────
echo ""
echo "Installing required packages..."
pip install -r requirements.txt

# ── 6. Check for Google API credentials ──────────────────────────────────────
CREDS_PATH="$HOME/Documents/API Cred/noble-aquifer-437514-k4-a50658fe7247.json"

echo ""
if [ -f "$CREDS_PATH" ]; then
    echo "✅ Google API credentials found."
else
    echo "⚠️  Google API credentials NOT found at:"
    echo "   $CREDS_PATH"
    echo ""
    echo "   To fix this:"
    echo "   1. Go to https://console.cloud.google.com"
    echo "   2. Download your Service Account JSON key"
    echo "   3. Place it at the path above"
fi

# ── 7. Done ───────────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo "  Setup complete!"
echo ""
echo "  To activate the environment next time:"
echo "  source venv/bin/activate"
echo ""
echo "  Then run any script:"
echo "  python etf_efficient_macd.py"
echo "========================================"
