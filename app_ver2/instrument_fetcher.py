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
        """Fetch specs for all instruments across all exchanges using bulk fetching."""
        self.client = httpx.AsyncClient(timeout=10.0)

        tasks = []
        for exchange_name, config in configs.items():
            # Fetch all instruments for this exchange in ONE call
            tasks.append(
                self._fetch_exchange_instruments(exchange_name, config.instruments)
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten results and store in self.specs
        success = 0
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch instruments: {result}")
                continue
            # result is dict[tuple[str, str], InstrumentSpec] here
            if isinstance(result, dict):
                success += len(result)
                self.specs.update(result)

        total_expected = sum(len(config.instruments) for config in configs.values())
        failed = total_expected - success
        logger.info(
            f"📊 Loaded {success}/{total_expected} instrument specs ({failed} failed)"
        )

    async def _make_request(
        self,
        url: str,
        params: dict | None = None,
        method: str = "GET",
    ) -> dict:
        """Make HTTP request with retry logic for rate limiting and transient errors.

        Args:
            url: Full URL to request
            params: Query parameters (optional)
            method: HTTP method (default: GET)

        Returns:
            Parsed JSON response

        Raises:
            httpx.HTTPStatusError: After max retries for non-retryable errors
            httpx.RequestError: After max retries for network errors
        """
        max_retries = 3
        base_delay = 1.0  # seconds

        for attempt in range(max_retries):
            try:
                if method == "GET":
                    resp = await self.client.get(url, params=params)  # pyright: ignore
                else:
                    resp = await self.client.request(method, url, params=params)  # pyright: ignore
                await asyncio.sleep(base_delay * 2)
                resp.raise_for_status()
                return resp.json()

            except httpx.HTTPStatusError as e:
                # Retry on rate limiting or server errors
                if e.response.status_code in (429, 500, 502, 503, 504):
                    if attempt < max_retries - 1:
                        delay = base_delay * (2**attempt)
                        logger.warning(
                            f"HTTP {e.response.status_code} for {url}, "
                            f"retrying in {delay}s (attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(delay)
                        continue
                raise

            except httpx.RequestError as e:
                # Retry on network errors
                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt)
                    logger.warning(
                        f"Request error for {url}: {e}, "
                        f"retrying in {delay}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(delay)
                    continue
                raise

        raise RuntimeError(f"Max retries exceeded for {url}")

    async def _fetch_exchange_instruments(
        self, exchange: str, instruments: list[str]
    ) -> dict[tuple[str, str], InstrumentSpec]:
        """Fetch all instruments for an exchange in bulk.

        Returns:
            Dict mapping (exchange, instrument) → InstrumentSpec
        """
        try:
            if exchange == "bybit":
                specs_dict = await self._fetch_all_bybit(instruments)
            elif exchange == "okx":
                specs_dict = await self._fetch_all_okx(instruments)
            elif exchange == "binance":
                specs_dict = await self._fetch_all_binance(instruments)
            elif exchange == "deribit":
                specs_dict = await self._fetch_all_deribit(instruments)
            elif exchange == "bitget":
                specs_dict = await self._fetch_all_bitget(instruments)
            else:
                raise ValueError(f"Unknown exchange: {exchange}")

            # Add exchange name to keys
            return {(exchange, inst): spec for inst, spec in specs_dict.items()}

        except Exception as e:
            logger.error(f"Failed to fetch {exchange} instruments: {e}")
            return {}

    async def _fetch_all_bybit(
        self, instruments: list[str]
    ) -> dict[str, InstrumentSpec]:
        """Fetch all Bybit instruments in one API call."""
        url = "https://api.bybit.com/v5/market/instruments-info"
        params = {"category": "linear", "limit": 1000}
        data = await self._make_request(url, params)

        if not data.get("result", {}).get("list"):
            return {}

        all_instruments = {}
        for info in data["result"]["list"]:
            symbol = info["symbol"]
            if symbol in instruments:
                all_instruments[symbol] = InstrumentSpec(
                    contract_size=1.0,
                    fee_pct=0.1,
                    min_order_qnt=info["lotSizeFilter"]["minOrderQty"],
                    qnt_step=info["lotSizeFilter"]["qtyStep"],
                    **info,
                )

        return all_instruments

    async def _fetch_all_okx(self, instruments: list[str]) -> dict[str, InstrumentSpec]:
        """Fetch all OKX instruments grouped by type."""
        # Group instruments by type
        swap_instruments = [i for i in instruments if "SWAP" in i or "-PERPETUAL" in i]
        futures_instruments = [
            i
            for i in instruments
            if len(i.split("-")) >= 3 and i not in swap_instruments
        ]
        spot_instruments = [
            i
            for i in instruments
            if i not in swap_instruments and i not in futures_instruments
        ]

        all_specs = {}

        # Fetch each type
        for inst_type, inst_list in [
            ("SWAP", swap_instruments),
            ("FUTURES", futures_instruments),
            ("SPOT", spot_instruments),
        ]:
            if not inst_list:
                continue

            url = "https://www.okx.com/api/v5/public/instruments"
            params = {"instType": inst_type}
            data = await self._make_request(url, params)

            if not data.get("data"):
                continue

            for info in data["data"]:
                inst_id = info["instId"]
                if inst_id in inst_list:
                    all_specs[inst_id] = InstrumentSpec(
                        contract_size=float(info.get("ctVal", 0.01)),
                        fee_pct=0.05,
                        min_order_qnt=info["minSz"],
                        qnt_step=info["tickSz"],
                        baseCoin=info["baseCcy"],
                        settleCoin=info["settleCcy"],
                    )
        return all_specs

    async def _fetch_all_binance(
        self, instruments: list[str]
    ) -> dict[str, InstrumentSpec]:
        """Fetch all Binance instruments in one API call."""
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = await self._make_request(url)

        all_specs = {}
        for s in data.get("symbols", []):
            symbol = s["symbol"]
            # Match against base symbol or full instrument
            for instrument in instruments:
                base_symbol = instrument.split("_")[0]
                if symbol == base_symbol or symbol == instrument:
                    size_filter = [
                        item
                        for item in s["filters"]
                        if item["filterType"] == "MARKET_LOT_SIZE"
                    ][0]

                    all_specs[instrument] = InstrumentSpec(
                        contract_size=1.0,
                        fee_pct=0.05,
                        settleCoin=s["marginAsset"],
                        baseCoin=s["baseAsset"],
                        min_order_qnt=size_filter["minQty"],
                        qnt_step=size_filter["stepSize"],
                    )
                    break

        return all_specs

    async def _fetch_all_deribit(
        self, instruments: list[str]
    ) -> dict[str, InstrumentSpec]:
        """Fetch all Deribit instruments grouped by currency."""
        # Extract unique currencies from instruments
        currencies = set()
        for inst in instruments:
            if "-" in inst:
                currencies.add(inst.split("-")[0].split("_")[1])

        all_specs = {}

        # Fetch instruments for each currency
        for currency in currencies:
            url = "https://www.deribit.com/api/v2/public/get_instruments"
            params = {"currency": currency, "kind": "future"}
            data = await self._make_request(url, params)

            if "result" not in data:
                continue

            for info in data["result"]:
                inst_name = info["instrument_name"]
                if inst_name in instruments:
                    all_specs[inst_name] = InstrumentSpec(
                        contract_size=float(info.get("contract_size", 1.0)),
                        fee_pct=0.05,
                        min_order_qnt=info["min_trade_amount"],
                        baseCoin=info["base_currency"],
                        settleCoin=info["settlement_currency"],
                        qnt_step=info["tick_size"],
                    )

        return all_specs

    async def _fetch_all_bitget(
        self, instruments: list[str]
    ) -> dict[str, InstrumentSpec]:
        """Fetch all Bitget instruments in one API call."""
        url = "https://api.bitget.com/api/v2/mix/market/contracts"
        params = {"productType": "usdt-futures"}
        data = await self._make_request(url, params)

        if not data.get("data"):
            return {}

        all_specs = {}
        for info in data["data"]:
            symbol = info["symbol"]
            if symbol in instruments:
                all_specs[symbol] = InstrumentSpec(
                    contract_size=float(info.get("sizeMultiplier", 1.0)),
                    fee_pct=0.06,
                    min_order_qnt=info["minTradeNum"],
                    qnt_step=info["pricePlace"],
                    baseCoin=info["baseCoin"],
                    settleCoin=info["quoteCoin"],
                )

        return all_specs

    def get_spec(self, exchange: str, instrument: str) -> InstrumentSpec:
        """Get spec for an instrument."""
        return self.specs.get((exchange, instrument))  # pyright: ignore[reportReturnType]

    async def close(self):
        """Close HTTP client."""
        if self.client:
            await self.client.aclose()
