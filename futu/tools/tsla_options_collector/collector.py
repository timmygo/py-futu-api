"""Collector pipeline for TSLA option chain."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, date
from typing import Iterable, List, Optional, Sequence, Set

from futu import (
    RET_OK,
    OpenQuoteContext,
    StockQuoteHandlerBase,
    SubType,
    SysNotifyHandlerBase,
    SysNotifyType,
    TickerHandlerBase,
)

from .storage import DuckDBStorage


@dataclass
class OptionFilterConfig:
    underlying_code: str = "US.TSLA"


class OptionSubscriptionManager:
    """Maintain active subscriptions and restore them after reconnect."""

    def __init__(self, quote_ctx: OpenQuoteContext, logger: logging.Logger) -> None:
        self._quote_ctx = quote_ctx
        self._logger = logger
        self._quote_push_codes: Set[str] = set()
        self._ticker_codes: Set[str] = set()
        self._last_connected: Optional[bool] = None

    def subscribe_quotes(
        self,
        codes: Sequence[str],
        *,
        push: bool = True,
        first_push: bool = True,
    ) -> None:
        code_list = sorted(set(codes))
        if not code_list:
            return
        ret, msg = self._quote_ctx.subscribe(
            code_list,
            [SubType.QUOTE],
            is_first_push=first_push,
            subscribe_push=push,
        )
        if ret != RET_OK:
            raise RuntimeError(f"Failed to subscribe quotes: {msg}")
        if push:
            self._quote_push_codes.update(code_list)
        self._logger.debug(
            "Subscribed quotes for %d codes (push=%s)", len(code_list), push
        )

    def unsubscribe_quotes(self, codes: Sequence[str]) -> None:
        code_list = sorted(set(codes))
        if not code_list:
            return
        ret, msg = self._quote_ctx.unsubscribe(code_list, [SubType.QUOTE])
        if ret != RET_OK:
            raise RuntimeError(f"Failed to unsubscribe quotes: {msg}")
        for code in code_list:
            self._quote_push_codes.discard(code)
        self._logger.debug("Unsubscribed quotes for %d codes", len(code_list))

    def subscribe_realtime(self, codes: Sequence[str]) -> None:
        code_list = sorted(set(codes))
        if not code_list:
            return
        ret, msg = self._quote_ctx.subscribe(
            code_list,
            [SubType.QUOTE, SubType.TICKER],
            is_first_push=True,
            subscribe_push=True,
        )
        if ret != RET_OK:
            raise RuntimeError(f"Failed to subscribe realtime streams: {msg}")
        self._quote_push_codes.update(code_list)
        self._ticker_codes.update(code_list)
        self._logger.info("Subscribed realtime streams for %d codes", len(code_list))

    def unsubscribe_all(self) -> None:
        if self._quote_push_codes:
            ret, msg = self._quote_ctx.unsubscribe(
                sorted(self._quote_push_codes), [SubType.QUOTE]
            )
            if ret != RET_OK:
                self._logger.warning("Failed to unsubscribe quotes: %s", msg)
        if self._ticker_codes:
            ret, msg = self._quote_ctx.unsubscribe(
                sorted(self._ticker_codes), [SubType.TICKER]
            )
            if ret != RET_OK:
                self._logger.warning("Failed to unsubscribe tickers: %s", msg)
        self._quote_push_codes.clear()
        self._ticker_codes.clear()

    def on_connection_state_change(self, connected: bool) -> None:
        if self._last_connected == connected:
            return
        self._last_connected = connected
        if not connected:
            self._logger.warning("Quote connection lost")
            return
        restored = self.restore_subscriptions()
        self._logger.info("断开→重连→已恢复 %d 条订阅", restored)

    def restore_subscriptions(self) -> int:
        expected = {
            SubType.QUOTE: set(self._quote_push_codes),
            SubType.TICKER: set(self._ticker_codes),
        }
        if not any(expected.values()):
            return 0
        ret, data = self._quote_ctx.query_subscription(is_all_conn=False)
        if ret != RET_OK:
            self._logger.warning("query_subscription failed: %s", data)
            return 0
        current = {
            subtype: set(data.get("sub_list", {}).get(subtype, []))
            for subtype in expected.keys()
        }
        restored = 0
        for subtype, codes in expected.items():
            missing = sorted(codes - current.get(subtype, set()))
            if not missing:
                continue
            ret, msg = self._quote_ctx.subscribe(
                missing,
                [subtype],
                is_first_push=True,
                subscribe_push=True,
            )
            if ret != RET_OK:
                self._logger.warning(
                    "Failed to restore %s subscriptions: %s", subtype, msg
                )
                continue
            restored += len(missing)
        return restored


class OptionFilter:
    """Filter TSLA option chain for the current week."""

    def __init__(
        self,
        quote_ctx: OpenQuoteContext,
        subscription_manager: OptionSubscriptionManager,
        logger: logging.Logger,
        config: OptionFilterConfig | None = None,
    ) -> None:
        self._quote_ctx = quote_ctx
        self._subscription_manager = subscription_manager
        self._logger = logger
        self._config = config or OptionFilterConfig()

    def fetch_current_week_option_codes(self) -> List[str]:
        ret, data = self._quote_ctx.get_option_expiration_date(
            self._config.underlying_code
        )
        if ret != RET_OK:
            raise RuntimeError(f"get_option_expiration_date failed: {data}")
        if data.empty:
            raise RuntimeError("No expiration dates returned for TSLA")
        today = datetime.utcnow().date()
        target_date = self._select_current_week(data["strike_time"].tolist(), today)
        self._logger.info("Using expiration %s", target_date.isoformat())
        ret, chain = self._quote_ctx.get_option_chain(
            self._config.underlying_code,
            start=target_date.isoformat(),
            end=target_date.isoformat(),
        )
        if ret != RET_OK:
            raise RuntimeError(f"get_option_chain failed: {chain}")
        if chain.empty:
            raise RuntimeError("Option chain is empty for selected expiration")
        codes = sorted(chain["code"].dropna().unique().tolist())
        self._logger.info("Fetched %d option contracts", len(codes))
        return codes

    def filter_by_delta(self, codes: Sequence[str]) -> List[str]:
        """Filter contracts by absolute delta in [0.1, 0.9]."""
        # Δ ∈ [+0.1,+0.9] ∪ [-0.9,-0.1]，不提供其他筛选项
        if not codes:
            return []
        self._logger.info("Subscribing %d contracts for delta snapshot", len(codes))
        self._subscription_manager.subscribe_quotes(
            codes, push=False, first_push=False
        )
        ret, quotes = self._quote_ctx.get_stock_quote(list(codes))
        if ret != RET_OK:
            raise RuntimeError(f"get_stock_quote failed: {quotes}")
        if quotes.empty:
            return []
        filtered = quotes[
            quotes["delta"].between(0.1, 0.9, inclusive="both")
            | quotes["delta"].between(-0.9, -0.1, inclusive="both")
        ]
        selected_codes = sorted(filtered["code"].dropna().unique().tolist())
        deselected = sorted(set(codes) - set(selected_codes))
        if deselected:
            self._logger.info(
                "Delta filter removed %d contracts", len(deselected)
            )
            self._subscription_manager.unsubscribe_quotes(deselected)
        else:
            self._logger.info("All contracts passed delta filter")
        return selected_codes

    def _select_current_week(self, strike_times: Iterable[str], today: date) -> date:
        parsed_dates: List[date] = []
        for value in strike_times:
            try:
                parsed = datetime.fromisoformat(str(value)).date()
                parsed_dates.append(parsed)
            except ValueError:
                continue
        if not parsed_dates:
            raise RuntimeError("Unable to parse strike_time values")
        current_week = today.isocalendar()[:2]
        this_week = [
            d for d in parsed_dates if d >= today and d.isocalendar()[:2] == current_week
        ]
        if this_week:
            return min(this_week)
        future = [d for d in parsed_dates if d >= today]
        if future:
            return min(future)
        return min(parsed_dates)


class OptionCollector:
    """Run the TSLA option stream collector."""

    def __init__(
        self,
        quote_ctx: OpenQuoteContext,
        storage: DuckDBStorage,
        subscription_manager: OptionSubscriptionManager,
        option_filter: OptionFilter,
        logger: logging.Logger,
    ) -> None:
        self._quote_ctx = quote_ctx
        self._storage = storage
        self._subscription_manager = subscription_manager
        self._option_filter = option_filter
        self._logger = logger

    def run(self, duration: Optional[int] = None) -> None:
        codes = self._option_filter.fetch_current_week_option_codes()
        selected = self._option_filter.filter_by_delta(codes)
        if not selected:
            self._logger.warning("No contracts matched delta criteria")
            return
        self._subscription_manager.subscribe_realtime(selected)
        self._subscription_manager.restore_subscriptions()

        quote_handler = _QuoteHandler(self._storage, self._logger)
        ticker_handler = _TickerHandler(self._storage, self._logger)
        connection_handler = _ConnectionHandler(
            self._subscription_manager, self._logger
        )
        self._quote_ctx.set_handler(quote_handler)
        self._quote_ctx.set_handler(ticker_handler)
        self._quote_ctx.set_handler(connection_handler)
        self._quote_ctx.start()
        self._logger.info("Collector started with %d contracts", len(selected))
        try:
            if duration is None:
                while True:
                    time.sleep(1)
            else:
                deadline = time.monotonic() + duration
                while time.monotonic() < deadline:
                    time.sleep(1)
        except KeyboardInterrupt:
            self._logger.info("Collector interrupted by user")
        finally:
            self._logger.info("Stopping collector")
            self._quote_ctx.stop()
            self._subscription_manager.unsubscribe_all()


class _QuoteHandler(StockQuoteHandlerBase):
    def __init__(self, storage: DuckDBStorage, logger: logging.Logger) -> None:
        super().__init__()
        self._storage = storage
        self._logger = logger

    def on_recv_rsp(self, rsp_pb):
        ret, frame = super().on_recv_rsp(rsp_pb)
        if ret != RET_OK:
            self._logger.error("Quote handler error: %s", frame)
            return ret, frame
        self._storage.append_quote(frame)
        self._logger.debug("Stored %d quote rows", len(frame))
        return ret, frame


class _TickerHandler(TickerHandlerBase):
    def __init__(self, storage: DuckDBStorage, logger: logging.Logger) -> None:
        super().__init__()
        self._storage = storage
        self._logger = logger

    def on_recv_rsp(self, rsp_pb):
        ret, frame = super().on_recv_rsp(rsp_pb)
        if ret != RET_OK:
            self._logger.error("Ticker handler error: %s", frame)
            return ret, frame
        self._storage.append_ticker(frame)
        self._logger.debug("Stored %d ticker rows", len(frame))
        return ret, frame


class _ConnectionHandler(SysNotifyHandlerBase):
    def __init__(
        self,
        subscription_manager: OptionSubscriptionManager,
        logger: logging.Logger,
    ) -> None:
        super().__init__()
        self._subscription_manager = subscription_manager
        self._logger = logger

    def on_recv_rsp(self, rsp_pb):
        ret, content = super().on_recv_rsp(rsp_pb)
        if ret != RET_OK:
            return ret, content
        notify_type, _, payload = content
        if notify_type == SysNotifyType.CONN_STATUS and payload is not None:
            connected = bool(payload.get("qot_logined"))
            self._subscription_manager.on_connection_state_change(connected)
            self._logger.debug("Connection status update: %s", connected)
        return ret, content

