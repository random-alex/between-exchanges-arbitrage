"""Configuration loader."""

from connectors.base import ConnectorConfig


class Config:
    """Application configuration constants."""

    # Position Management
    MIN_ROI_TO_OPEN = 2.0
    STOP_LOSS_PCT = -10.0
    TARGET_CONVERGENCE_PCT = 0.1
    MAX_HOLD_TIME_HOURS = 24
    MIN_SPREAD_PCT = 1.5

    # Spread Calculation
    CAPITAL = 100.0
    LEVERAGE = 10.0
    MIN_SPREAD_THRESHOLD = 0.05
    MIN_ROI_FOR_LOGGING = 0.5

    # Monitoring
    QUEUE_TIMEOUT = 60.0
    STATS_INTERVAL = 300.0
    SPREAD_CHECK_INTERVAL = 1.0
    LOG_COOLDOWN = 60.0
    LOGGER_WINDOW = 10.0


def load_config() -> dict[str, ConnectorConfig]:
    """Load connector configurations."""

    bybit_instruments = [
        # "BTCUSDT-26DEC25",
        "SOLUSDT",
        # "BTCUSDT",
        # "ETHUSDT",
        "ADAUSDT",
        "XRPUSDT",
        "BNBUSDT",
        "AVAXUSDT",
        "DOGEUSDT",
        "DOTUSDT",
        "LINKUSDT",
        "LTCUSDT",
        "UNIUSDT",
        "TRXUSDT",
        "NEARUSDT",
        "ALGOUSDT",
    ]

    okx_instruments = [
        # "BTC-USDT-251226",
        "SOL-USDT-SWAP",
        # "BTC-USDT-SWAP",
        # "ETH-USDT-SWAP",
        "ADA-USDT-SWAP",
        "XRP-USDT-SWAP",
        "BNB-USDT-SWAP",
        "AVAX-USDT-SWAP",
        "DOGE-USDT-SWAP",
        "DOT-USDT-SWAP",
        "LINK-USDT-SWAP",
        "LTC-USDT-SWAP",
        "UNI-USDT-SWAP",
        "TRX-USDT-SWAP",
        "NEAR-USDT-SWAP",
        "ALGO-USDT-SWAP",
    ]

    # Binance perpetual futures symbols
    binance_instruments = [
        # "BTCUSDT_251226",
        "SOLUSDT",
        "ADAUSDT",
        "XRPUSDT",
        "BNBUSDT",
        "AVAXUSDT",
        "DOGEUSDT",
        "DOTUSDT",
        "LINKUSDT",
        "LTCUSDT",
        "UNIUSDT",
        "TRXUSDT",
        "NEARUSDT",
        "ALGOUSDT",
    ]

    # Deribit options and futures
    deribit_instruments = [
        # "BTC-26DEC25",
        "SOL_USDC-PERPETUAL",
        "ADA_USDC-PERPETUAL",
        "XRP_USDC-PERPETUAL",
        "BNB_USDC-PERPETUAL",
        "AVAX_USDC-PERPETUAL",
        "DOGE_USDC-PERPETUAL",
        "DOT_USDC-PERPETUAL",
        "LINK_USDC-PERPETUAL",
        "LTC_USDC-PERPETUAL",
        "UNI_USDC-PERPETUAL",
        "TRX_USDC-PERPETUAL",
        "NEAR_USDC-PERPETUAL",
        "ALGO_USDC-PERPETUAL",
    ]

    return {
        "bybit": ConnectorConfig(
            name="bybit",
            instruments=bybit_instruments,
            initial_reconnect_delay=3.0,
            max_reconnect_delay=60.0,
            max_retries=10,
        ),
        "okx": ConnectorConfig(
            name="okx",
            instruments=okx_instruments,
            initial_reconnect_delay=3.0,
            max_reconnect_delay=60.0,
            max_retries=10,
        ),
        "binance": ConnectorConfig(
            name="binance",
            instruments=binance_instruments,
            initial_reconnect_delay=3.0,
            max_reconnect_delay=60.0,
            max_retries=10,
        ),
        "deribit": ConnectorConfig(
            name="deribit",
            instruments=deribit_instruments,
            initial_reconnect_delay=3.0,
            max_reconnect_delay=60.0,
            max_retries=10,
        ),
    }
