#!/usr/bin/env bash
if [ -f "venv/bin/activate" ]; then
  source venv/bin/activate
fi
echo "[Scout] Launching CORE-Scout..."
python run_app.py --config config/settings_offline.json
