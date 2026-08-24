"""
NSE NIFTY EOD Option Data Downloader
=====================================
Downloads NIFTY option chain data (CE & PE) for ATM ± N strikes
from NSE India's public API.

Usage:
    from nse_option_downloader import NSEOptionDownloader

    downloader = NSEOptionDownloader()
    df = downloader.download()

Author : Auto-generated design
Version: 1.0.0
"""

import logging
import os
import time
from datetime import datetime, date
from typing import Optional, List, Dict, Any

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("nse_option_downloader")

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class NSEError(Exception):
    """Base exception for all NSE-related errors."""


class NSESessionError(NSEError):
    """Raised when session/cookie establishment fails."""


class NSEFetchError(NSEError):
    """Raised when the API request fails after retries."""


class NSEDataError(NSEError):
    """Raised when the API response has unexpected structure."""


class NSERateLimitError(NSEError):
    """Raised when NSE rate-limits or blocks the request."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_CONFIG: Dict[str, Any] = {
    "BASE_URL": "https://www.nseindia.com",
    "OPTION_CHAIN_URL": "/api/option-chain-indices",
    "SYMBOL": "NIFTY",
    "STRIKE_STEP": 50,
    "STRIKE_COUNT": 10,
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 2,           # seconds between retries
    "REQUEST_TIMEOUT": 30,      # seconds
    "MIN_REQUEST_INTERVAL": 1.5,  # seconds between any two requests
}

_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
}

_API_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": _HEADERS["User-Agent"],
}


# ---------------------------------------------------------------------------
# Session Manager
# ---------------------------------------------------------------------------

class NSESession:
    """
    Manages an HTTP session with NSE India, handling cookie
    acquisition, refresh, and rate limiting.
    """

    def __init__(self, config: Dict[str, Any]):
        self._config = config
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)
        self._last_request_time: float = 0.0
        self._cookies_valid: bool = False

    # -- public --

    def establish(self) -> None:
        """Visit NSE homepage to capture essential cookies."""
        base = self._config["BASE_URL"]
        max_retries = self._config["MAX_RETRIES"]

        for attempt in range(1, max_retries + 1):
            try:
                logger.info("Establishing session (attempt %d/%d)...", attempt, max_retries)
                self._throttle()
                resp = self._session.get(
                    base,
                    timeout=self._config["REQUEST_TIMEOUT"],
                )
                resp.raise_for_status()
                self._cookies_valid = True
                logger.info(
                    "Session established — %d cookies captured.",
                    len(self._session.cookies),
                )
                return
            except requests.RequestException as exc:
                logger.warning("Session attempt %d failed: %s", attempt, exc)
                if attempt < max_retries:
                    time.sleep(self._config["RETRY_DELAY"] * attempt)

        raise NSESessionError(
            f"Could not establish NSE session after {max_retries} attempts."
        )

    def get_json(self, url: str, params: Optional[dict] = None) -> dict:
        """
        Perform a GET request that expects a JSON response.
        Handles session refresh on 401/403 and retries on failure.
        """
        max_retries = self._config["MAX_RETRIES"]

        for attempt in range(1, max_retries + 1):
            if not self._cookies_valid:
                self.establish()

            try:
                self._throttle()
                # Switch to API-specific headers for JSON endpoints
                resp = self._session.get(
                    url,
                    params=params,
                    headers=_API_HEADERS,
                    timeout=self._config["REQUEST_TIMEOUT"],
                )

                if resp.status_code in (401, 403):
                    logger.warning(
                        "Received %d — refreshing session...", resp.status_code
                    )
                    self._cookies_valid = False
                    time.sleep(self._config["RETRY_DELAY"])
                    continue

                if resp.status_code == 429:
                    wait = self._config["RETRY_DELAY"] * attempt * 2
                    logger.warning("Rate-limited (429). Backing off %ds...", wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp.json()

            except (requests.RequestException, ValueError) as exc:
                logger.warning("Request attempt %d failed: %s", attempt, exc)
                if attempt < max_retries:
                    time.sleep(self._config["RETRY_DELAY"] * attempt)

        raise NSEFetchError(
            f"Failed to fetch {url} after {max_retries} attempts."
        )

    # -- private --

    def _throttle(self) -> None:
        """Enforce minimum interval between consecutive requests."""
        elapsed = time.time() - self._last_request_time
        wait = self._config["MIN_REQUEST_INTERVAL"] - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_time = time.time()


# ---------------------------------------------------------------------------
# Option Chain Fetcher
# ---------------------------------------------------------------------------

class OptionChainFetcher:
    """Fetches the raw option chain JSON from the NSE API."""

    def __init__(self, session: NSESession, config: Dict[str, Any]):
        self._session = session
        self._config = config

    def fetch(self, symbol: str) -> dict:
        """
        Fetch the complete option chain for the given index symbol.

        Returns the parsed JSON dict with keys:
          - records.data          : list of strike records
          - records.underlyingValue : spot price
          - records.expiryDates   : list of expiry dates
          - records.strikePrices  : list of available strikes
        """
        url = (
            f"{self._config['BASE_URL']}"
            f"{self._config['OPTION_CHAIN_URL']}"
        )
        # Set the referer to the option chain page
        self._session._session.headers["Referer"] = (
            f"{self._config['BASE_URL']}/option-chain-indices"
        )
        data = self._session.get_json(url, params={"symbol": symbol})

        # Validate essential keys
        if "records" not in data:
            raise NSEDataError(
                "API response missing 'records' key. "
                "Response keys: " + str(list(data.keys()))
            )
        records = data["records"]
        for key in ("data", "underlyingValue", "expiryDates", "strikePrices"):
            if key not in records:
                raise NSEDataError(f"records missing '{key}' field.")

        logger.info(
            "Option chain fetched — %d records, %d expiries, spot=%.2f",
            len(records["data"]),
            len(records["expiryDates"]),
            records["underlyingValue"],
        )
        return data


# ---------------------------------------------------------------------------
# ATM Calculator
# ---------------------------------------------------------------------------

class ATMCalculator:
    """Determines the ATM strike and builds the strike range."""

    @staticmethod
    def get_spot_price(chain_data: dict) -> float:
        """Extract the underlying spot price from the option chain."""
        try:
            return float(chain_data["records"]["underlyingValue"])
        except (KeyError, TypeError, ValueError) as exc:
            raise NSEDataError(f"Cannot extract spot price: {exc}") from exc

    @staticmethod
    def get_atm_strike(spot: float, step: int) -> float:
        """Round the spot price to the nearest strike step."""
        return round(spot / step) * step

    @staticmethod
    def get_strike_range(
        atm: float, step: int, count: int
    ) -> List[float]:
        """
        Generate a list of strikes: ATM - count*step ... ATM ... ATM + count*step.
        Returns (2*count + 1) strikes.
        """
        return [atm + (i * step) for i in range(-count, count + 1)]


# ---------------------------------------------------------------------------
# Data Transformer
# ---------------------------------------------------------------------------

class DataTransformer:
    """Flattens the nested option chain JSON into a clean DataFrame."""

    # Fields to extract from each CE/PE record
    _FIELDS = [
        "openInterest",
        "changeinOpenInterest",
        "totalTradedVolume",
        "impliedVolatility",
        "lastPrice",
        "change",
        "pChange",
        "totalBuyQuantity",
        "totalSellQuantity",
        "bidQty",
        "bidprice",
        "askQty",
        "askPrice",
        "underlyingValue",
    ]

    # Readable column names (same order as _FIELDS)
    _COLUMNS = [
        "open_interest",
        "change_in_oi",
        "total_traded_vol",
        "implied_volatility",
        "last_price",
        "change",
        "pct_change",
        "total_buy_qty",
        "total_sell_qty",
        "bid_qty",
        "bid_price",
        "ask_qty",
        "ask_price",
        "underlying_value",
    ]

    def extract(
        self,
        chain_data: dict,
        strikes: List[float],
        expiry: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract CE and PE records for the given strikes and optional expiry.

        Parameters
        ----------
        chain_data : dict
            Full API response from OptionChainFetcher.
        strikes : list[float]
            List of strike prices to include.
        expiry : str or None
            If provided, filter to this expiry date string (e.g. "07-Aug-2026").
            If None, include all expiries.

        Returns
        -------
        pd.DataFrame
        """
        records = chain_data["records"]["data"]
        strike_set = set(strikes)
        rows: List[dict] = []

        for rec in records:
            sp = rec.get("strikePrice")
            if sp not in strike_set:
                continue

            exp = rec.get("expiryDate", "")
            if expiry and exp != expiry:
                continue

            # Process CE side
            if "CE" in rec:
                rows.append(self._flatten(rec["CE"], sp, exp, "CE"))

            # Process PE side
            if "PE" in rec:
                rows.append(self._flatten(rec["PE"], sp, exp, "PE"))

        if not rows:
            logger.warning("No records matched the filter criteria.")
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # Sort for readability: expiry → strike → option_type
        df.sort_values(
            ["expiry_date", "strike_price", "option_type"],
            ascending=[True, True, True],
            inplace=True,
        )
        df.reset_index(drop=True, inplace=True)

        logger.info("Extracted %d option records.", len(df))
        return df

    def _flatten(
        self, opt: dict, strike: float, expiry: str, opt_type: str
    ) -> dict:
        """Flatten a single CE or PE record dict into a flat row."""
        row = {
            "strike_price": strike,
            "expiry_date": expiry,
            "option_type": opt_type,
        }
        for field, col in zip(self._FIELDS, self._COLUMNS):
            row[col] = opt.get(field, None)
        return row


# ---------------------------------------------------------------------------
# CSV Exporter
# ---------------------------------------------------------------------------

class CSVExporter:
    """Exports a DataFrame to a date-stamped CSV file."""

    @staticmethod
    def export(
        df: pd.DataFrame,
        output_dir: str,
        symbol: str,
        tag: Optional[str] = None,
    ) -> str:
        """
        Write the DataFrame to a CSV file.

        Returns the full path of the written file.
        """
        os.makedirs(output_dir, exist_ok=True)

        today = date.today().isoformat()
        suffix = f"_{tag}" if tag else ""
        filename = f"{symbol}_options_{today}{suffix}.csv"
        filepath = os.path.join(output_dir, filename)

        df.to_csv(filepath, index=False)
        logger.info("Exported %d rows → %s", len(df), filepath)
        return filepath


# ---------------------------------------------------------------------------
# Facade — NSEOptionDownloader
# ---------------------------------------------------------------------------

class NSEOptionDownloader:
    """
    High-level facade for downloading NIFTY option chain data.

    Parameters
    ----------
    symbol : str
        Index symbol (default "NIFTY"). Also supports "BANKNIFTY", "FINNIFTY".
    strike_step : int
        Gap between consecutive strike prices (default 50 for NIFTY).
    strike_count : int
        Number of strikes above and below ATM to include (default 10).
    expiry : str
        Expiry filter:
          - "nearest" : only the nearest expiry (default)
          - "all"     : all available expiries
          - "DD-Mon-YYYY" : a specific expiry date string
    output_dir : str or None
        Directory for CSV output. None skips file export.

    Example
    -------
    >>> dl = NSEOptionDownloader(output_dir="./data")
    >>> df = dl.download()
    >>> print(dl.atm_strike, len(df))
    24400.0  42
    """

    # Strike steps per symbol
    _STRIKE_STEPS = {
        "NIFTY": 50,
        "BANKNIFTY": 100,
        "FINNIFTY": 50,
        "NIFTY BANK": 100,
        "MIDCPNIFTY": 25,
    }

    def __init__(
        self,
        symbol: str = "NIFTY",
        strike_step: Optional[int] = None,
        strike_count: int = 10,
        expiry: str = "nearest",
        output_dir: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        self._symbol = symbol.upper()
        self._strike_step = strike_step or self._STRIKE_STEPS.get(self._symbol, 50)
        self._strike_count = strike_count
        self._expiry_filter = expiry
        self._output_dir = output_dir

        # Merge user config with defaults
        self._config = {**DEFAULT_CONFIG, **(config or {})}
        self._config["SYMBOL"] = self._symbol

        # Internal state (populated after download)
        self.spot_price: Optional[float] = None
        self.atm_strike: Optional[float] = None
        self.strikes: Optional[List[float]] = None
        self.expiry_dates: Optional[List[str]] = None

        # Components
        self._session = NSESession(self._config)
        self._fetcher = OptionChainFetcher(self._session, self._config)
        self._calculator = ATMCalculator()
        self._transformer = DataTransformer()
        self._exporter = CSVExporter()

    def download(self) -> pd.DataFrame:
        """
        Execute the full download pipeline:
          1. Establish NSE session
          2. Fetch option chain
          3. Calculate ATM and strike range
          4. Transform to DataFrame
          5. Optionally export CSV

        Returns
        -------
        pd.DataFrame
            Option chain data with CE and PE records.
        """
        logger.info("=" * 60)
        logger.info("NSE Option Downloader — %s", self._symbol)
        logger.info("=" * 60)

        # Step 1: Session
        self._session.establish()

        # Step 2: Fetch
        chain_data = self._fetcher.fetch(self._symbol)

        # Step 3: ATM calculation
        self.spot_price = self._calculator.get_spot_price(chain_data)
        self.atm_strike = self._calculator.get_atm_strike(
            self.spot_price, self._strike_step
        )
        self.strikes = self._calculator.get_strike_range(
            self.atm_strike, self._strike_step, self._strike_count
        )
        self.expiry_dates = chain_data["records"]["expiryDates"]

        logger.info("Spot: %.2f | ATM: %.0f | Range: %.0f – %.0f",
                     self.spot_price, self.atm_strike,
                     self.strikes[0], self.strikes[-1])

        # Step 4: Resolve expiry filter
        expiry = self._resolve_expiry(self.expiry_dates)
        logger.info("Expiry filter: %s", expiry or "ALL")

        # Step 5: Transform
        df = self._transformer.extract(chain_data, self.strikes, expiry)

        # Add ATM flag
        if not df.empty and self.atm_strike is not None:
            df["atm_flag"] = df["strike_price"] == self.atm_strike

        # Step 6: Export (optional)
        if self._output_dir and not df.empty:
            filepath = self._exporter.export(
                df, self._output_dir, self._symbol
            )
            logger.info("CSV saved: %s", filepath)

        logger.info("Download complete — %d records.", len(df))
        return df

    def _resolve_expiry(self, expiry_dates: List[str]) -> Optional[str]:
        """Resolve the expiry filter to a concrete expiry string or None."""
        filt = self._expiry_filter.strip().lower()

        if filt == "all":
            return None

        if filt == "nearest":
            if not expiry_dates:
                raise NSEDataError("No expiry dates found in API response.")
            # NSE returns expiry dates sorted nearest-first
            return expiry_dates[0]

        # Assume it's a specific date string like "07-Aug-2026"
        if self._expiry_filter in expiry_dates:
            return self._expiry_filter

        # Try to match loosely
        for exp in expiry_dates:
            if self._expiry_filter.lower() in exp.lower():
                return exp

        raise NSEDataError(
            f"Expiry '{self._expiry_filter}' not found. "
            f"Available: {expiry_dates[:5]}..."
        )

    def get_summary(self) -> Dict[str, Any]:
        """Return a summary dict of the last download run."""
        return {
            "symbol": self._symbol,
            "spot_price": self.spot_price,
            "atm_strike": self.atm_strike,
            "strike_range": (
                (self.strikes[0], self.strikes[-1]) if self.strikes else None
            ),
            "strike_count": len(self.strikes) if self.strikes else 0,
            "expiry_dates": self.expiry_dates,
            "timestamp": datetime.now().isoformat(),
        }


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main():
    """Command-line interface for quick downloads."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Download NIFTY EOD option chain data from NSE India."
    )
    parser.add_argument(
        "--symbol", default="NIFTY",
        help="Index symbol (default: NIFTY)"
    )
    parser.add_argument(
        "--strikes", type=int, default=10,
        help="Number of strikes above/below ATM (default: 10)"
    )
    parser.add_argument(
        "--expiry", default="nearest",
        help="Expiry filter: 'nearest', 'all', or 'DD-Mon-YYYY'"
    )
    parser.add_argument(
        "--output", default="./data",
        help="Output directory for CSV (default: ./data)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging"
    )
    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(name)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    downloader = NSEOptionDownloader(
        symbol=args.symbol,
        strike_count=args.strikes,
        expiry=args.expiry,
        output_dir=args.output,
    )

    df = downloader.download()

    # Print summary
    summary = downloader.get_summary()
    print("\n" + "=" * 50)
    print(f"  Symbol       : {summary['symbol']}")
    print(f"  Spot Price   : {summary['spot_price']}")
    print(f"  ATM Strike   : {summary['atm_strike']}")
    print(f"  Strike Range : {summary['strike_range']}")
    print(f"  Records      : {len(df)}")
    print("=" * 50)

    if not df.empty:
        print("\nSample data (first 10 rows):")
        print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
