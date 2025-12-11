"""Configuration loader."""

from connectors.base import ConnectorConfig


def load_config() -> dict[str, ConnectorConfig]:
    """Load connector configurations."""

    bybit_instruments = [
        # "BTCUSDT-12DEC25",
        # "BTCUSDT-19DEC25",
        # "BTCUSDT-26DEC25",
        # "BTCUSDT-30JAN26",
        # "BTCUSDT-27MAR26",
        # "BTCUSDT-26JUN26",
        # "ETHUSDT-12DEC25",
        # "ETHUSDT-19DEC25",
        # "ETHUSDT-26DEC25",
        # "ETHUSDT-27MAR26",
        "SOLUSDT",
        # "BTCUSDT",
        # "ETHUSDT",
        "ADAUSDT",
        "XRPUSDT",
    ]

    okx_instruments = [
        # "BTC-USDT-251226",
        # "BTC-USDT-251212",
        # "BTC-USDT-251219",
        # "BTC-USDT-260130",
        # "BTC-USDT-260327",
        # "BTC-USDT-260626",
        # "ETH-USDT-251226",
        # "ETH-USDT-251212",
        # "ETH-USDT-251219",
        # "ETH-USDT-260327",
        "SOL-USDT-SWAP",
        # "BTC-USDT-SWAP",
        # "ETH-USDT-SWAP",
        "ADA-USDT-SWAP",
        "XRP-USDT-SWAP",
    ]

    # Binance perpetual futures symbols
    binance_instruments = [
        # "BTCUSDT_251226",
        # "BTCUSDT_260327",
        # "ETHUSDT_251226",
        # "ETHUSDT_260327",
        "SOLUSDT",
        "ADAUSDT",
        "XRPUSDT",
        # "BTCUSDT",
        # "ETHUSDT",
    ]

    # Deribit options and futures
    deribit_instruments = [
        # "BTC-12DEC25",
        # "BTC-19DEC25",
        # "BTC-26DEC25",
        # "BTC-30JAN26",
        # "BTC-27MAR26",
        # "BTC-26JUN26",
        # "ETH-12DEC25",
        # "ETH-19DEC25",
        # "ETH-26DEC25",
        # "ETH-27MAR26",
        "SOL_USDC-PERPETUAL",
        "ADA_USDC-PERPETUAL",
        # "BTC_USDC-PERPETUAL",
        # "ETH_USDC-PERPETUAL",
        "XRP_USDC-PERPETUAL",
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
