#!/bin/bash
# Run the TX Boil Water Notice scraper and update the map data.
# Schedule with cron:
#   crontab -e
#   0 7 * * * /path/to/run_daily.sh >> /path/to/scraper.log 2>&1
#
# Or run manually:
#   ./run_daily.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== TX BWN Scraper Run: $(date) ==="
python3 tx_boil_water_scraper.py

echo ""
echo "To view the map:"
echo "  cd '$SCRIPT_DIR' && python3 -m http.server 8000"
echo "  Then open http://localhost:8000"
echo ""
