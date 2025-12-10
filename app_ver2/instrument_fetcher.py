"""Fetch instrument specifications from exchange APIs."""

import asyncio
import logging
from typing import Dict, Tuple
import httpx
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


class InstrumentSpec(BaseModel):
    contract_size: float
    fee_pct: float
    min_order_qnt: float
    qnt_step: float
    settle_coin: str = Field(alias="settleCoin")
    base_coin: str = Field(alias="baseCoin")


class InstrumentFetcher:
    """Fetch and store instrument specs from exchanges."""

    def __init__(self):
        self.specs: Dict[Tuple[str, str], InstrumentSpec] = {}
        self.client = None

    async def fetch_all(self, configs: dict) -> None:
        """Fetch specs for all instruments across all exchanges."""
        self.client = httpx.AsyncClient(timeout=10.0)

        tasks = []
        for exchange_name, config in configs.items():
            for instrument in config.instruments:
                tasks.append(self._fetch_instrument(exchange_name, instrument))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log summary
        success = sum(1 for r in results if not isinstance(r, Exception))
        failed = len(results) - success
        logger.info(
            f"📊 Loaded {success}/{len(results)} instrument specs ({failed} fallbacks)"
        )

    async def _fetch_instrument(self, exchange: str, instrument: str) -> None:
        """Fetch spec for a single instrument."""
        try:
            if exchange == "bybit":
                spec = await self._fetch_bybit(instrument)
            elif exchange == "okx":
                spec = await self._fetch_okx(instrument)
            elif exchange == "binance":
                spec = await self._fetch_binance(instrument)
            elif exchange == "deribit":
                spec = await self._fetch_deribit(instrument)
            else:
                raise ValueError(f"Unknown exchange: {exchange}")

            self.specs[(exchange, instrument)] = spec
            logger.debug(f"✅ Fetched {exchange}/{instrument}: {spec}")

        except Exception as e:
            raise ValueError(f"Failed {exchange}/{instrument}: {e}")

    async def _fetch_bybit(self, instrument: str) -> InstrumentSpec:
        """Fetch Bybit instrument spec."""
        url = "https://api.bybit.com/v5/market/instruments-info"
        params = {"category": "linear", "symbol": instrument}

        resp = await self.client.get(  # pyright: ignore[reportOptionalMemberAccess]
            url, params=params
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("result", {}).get("list"):
            raise ValueError(f"Instrument not found: {instrument}")

        info = data["result"]["list"][0]
        # Contract size for linear perpetuals/futures
        contract_size = 1.0

        # Bybit taker fee is typically 0.1% for USDT perpetuals
        fee_pct = 0.1

        return InstrumentSpec(
            contract_size=contract_size,
            fee_pct=fee_pct,
            min_order_qnt=info["lotSizeFilter"]["minOrderQty"],
            qnt_step=info["lotSizeFilter"]["qtyStep"],
            **info,
        )

    async def _fetch_okx(self, instrument: str) -> InstrumentSpec:
        """Fetch OKX instrument spec."""
        # Determine instrument type from format
        if "SWAP" in instrument or "-PERPETUAL" in instrument:
            inst_type = "SWAP"
        elif len(instrument.split("-")) >= 3:  # BTC-USDT-251226
            inst_type = "FUTURES"
        else:
            inst_type = "SPOT"

        url = "https://www.okx.com/api/v5/public/instruments"
        params = {"instType": inst_type, "instId": instrument}

        resp = await self.client.get(  # pyright: ignore[reportOptionalMemberAccess]
            url, params=params
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("data"):
            raise ValueError(f"Instrument not found: {instrument}")

        info = data["data"][0]
        contract_size = float(info.get("ctVal", 0.01))

        # OKX taker fee is 0.05% for most instruments
        fee_pct = 0.05

        return InstrumentSpec(
            contract_size=contract_size,
            fee_pct=fee_pct,
            min_order_qnt=info["minSz"],
            qnt_step=info["tickSz"],
            baseCoin=info["baseCcy"],
            settleCoin=info["settleCcy"],
        )

    async def _fetch_binance(self, instrument: str) -> InstrumentSpec:
        """Fetch Binance instrument spec."""
        # Binance futures use symbol format like BTCUSDT_251226 or BTCUSDT
        base_symbol = instrument.split("_")[0]

        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"

        resp = await self.client.get(url)  # pyright: ignore[reportOptionalMemberAccess]
        resp.raise_for_status()
        data = resp.json()

        # Find matching symbol
        symbol_info = None
        for s in data.get("symbols", []):
            if s["symbol"] == base_symbol or s["symbol"] == instrument:
                symbol_info = s
                break

        if not symbol_info:
            raise ValueError(f"Instrument not found: {instrument}")

        # Binance contract size is typically 1 USD for USDT-margined
        contract_size = 1.0
        size_filter = [
            item
            for item in symbol_info["filters"]
            if item["filterType"] == "MARKET_LOT_SIZE"
        ][0]
        # Binance taker fee is 0.05% for futures
        fee_pct = 0.05

        return InstrumentSpec(
            contract_size=contract_size,
            fee_pct=fee_pct,
            settleCoin=symbol_info["marginAsset"],
            baseCoin=symbol_info["baseAsset"],
            min_order_qnt=size_filter["minQty"],
            qnt_step=size_filter["stepSize"],
        )

    async def _fetch_deribit(self, instrument: str) -> InstrumentSpec:
        """Fetch Deribit instrument spec."""
        url = "https://www.deribit.com/api/v2/public/get_instrument"
        params = {"instrument_name": instrument}

        resp = await self.client.get(  # pyright: ignore[reportOptionalMemberAccess]
            url, params=params
        )
        resp.raise_for_status()
        data = resp.json()

        if "result" not in data:
            raise ValueError(f"Instrument not found: {instrument}")

        info = data["result"]
        contract_size = float(info.get("contract_size", 1.0))

        # Deribit taker fee is 0.05% for futures
        fee_pct = 0.05

        return InstrumentSpec(
            contract_size=contract_size,
            fee_pct=fee_pct,
            min_order_qnt=info["min_trade_amount"],
            baseCoin=info["base_currency"],
            settleCoin=info["settlement_currency"],
            qnt_step=info["tick_size"],
        )

    def get_spec(self, exchange: str, instrument: str) -> InstrumentSpec:
        """Get spec for an instrument."""
        return self.specs.get((exchange, instrument))  # pyright: ignore[reportReturnType]

    async def close(self):
        """Close HTTP client."""
        if self.client:
            await self.client.aclose()
