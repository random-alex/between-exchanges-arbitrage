# Simple between exchanges arbitrage system

# TL;DR
1. Listen to the prices from different exchanges using websockets.
2. Normilize and checkes if there is an arbitrage opportunity.
3. Execute a trade if there is one - currently just logs the trade.

Basic commands:
1. Init the env

    `uv sync`
2. Run the the logger

    `PYTHONPATH=$(pwd) uv run python app/connectors/run_all.py`
