from passivbot import Passivbot, logging
from uuid import uuid4
import ccxt.pro as ccxt_pro
import ccxt.async_support as ccxt_async
import pprint
import asyncio
import traceback
import numpy as np
import passivbot_rust as pbr
from collections import defaultdict
import time
from pure_funcs import (
    multi_replace,
    floatify,
    ts_to_date_utc,
    calc_hash,
    determine_pos_side_ccxt,
    symbol_to_coin,
    flatten,
)
from procedures import print_async_exception, utc_ms, assert_correct_ccxt_version

assert_correct_ccxt_version(ccxt=ccxt_async)


class BybitBot(Passivbot):
    def __init__(self, config: dict):
        # [수정 1] recv_window 값을 안전한 범위(20초)로 설정
        self.recv_window = 20000  
        super().__init__(config)
        self.last_time_sync = 0  # 마지막으로 시간을 맞춘 시각 저장용 변수
        

    def create_ccxt_sessions(self):
        self.ccp = getattr(ccxt_pro, self.exchange)(
            {
                "apiKey": self.user_info["key"],
                "secret": self.user_info["secret"],
                "password": self.user_info["passphrase"],
                "headers": {"referer": self.broker_code} if self.broker_code else {},
                'options': {
                    'adjustForTimeDifference': True,
                    "recvWindow": self.recv_window
                },
            }
        )
        self.cca = getattr(ccxt_async, self.exchange)(
            {
                "apiKey": self.user_info["key"],
                "secret": self.user_info["secret"],
                "password": self.user_info["passphrase"],
                "headers": {"referer": self.broker_code} if self.broker_code else {},
                'options': {
                    'adjustForTimeDifference': True,  # exchange-specific option
                    "recvWindow": self.recv_window
                },
            }
        )

        

    def set_market_specific_settings(self):
        super().set_market_specific_settings()
        for symbol in self.markets_dict:
            elm = self.markets_dict[symbol]
            self.symbol_ids[symbol] = elm["id"]
            self.min_costs[symbol] = (
                0.1 if elm["limits"]["cost"]["min"] is None else elm["limits"]["cost"]["min"]
            )
            self.min_qtys[symbol] = elm["limits"]["amount"]["min"]
            self.qty_steps[symbol] = elm["precision"]["amount"]
            self.price_steps[symbol] = elm["precision"]["price"]
            self.c_mults[symbol] = elm["contractSize"]

    async def resync_exchange_time(self):
        try:
            logging.info("⏳ Re-syncing time difference with Bybit server...")
            # ccxt_async와 ccxt_pro 양쪽 모두 시간 오차를 갱신
            await self.cca.load_time_difference()
            if hasattr(self, 'ccp'):
                await self.ccp.load_time_difference()
            self.last_time_sync = time.time()
            logging.info("✅ Time re-synced successfully.")
        except Exception as e:
            logging.error(f"⚠️ Failed to resync time: {e}")

    # [수정] watch_orders 메서드 (핵심 수정 부분)
    # [수정] watch_orders 함수 시작 부분에 시간 보정 로직 추가
    async def watch_orders(self):
        # 1. 봇 시작 시 최초 1회 시간 동기화 (강제)
        try:
            logging.info("⏳ Synchronizing time with Bybit server...")
            await self.cca.load_time_difference() # 여기서 오차(1.9초)를 계산해냅니다.
            logging.info(f"✅ Time synced. Offset: {self.cca.time_difference}ms")
        except Exception as e:
            logging.error(f"⚠️ Initial time sync failed: {e}")

        while True:
            try:
                # 2. 주기적 시간 동기화 (1시간마다) - 이전 답변의 로직 유지
                if hasattr(self, 'last_time_sync') and time.time() - self.last_time_sync > 3600:
                    await self.resync_exchange_time()

                if self.stop_websocket:
                    break
                
                res = await self.ccp.watch_orders()
                
                for i in range(len(res)):
                    res[i]["position_side"] = determine_pos_side_ccxt(res[i])
                    res[i]["qty"] = res[i]["amount"]
                self.handle_order_update(res)

            except Exception as e:
                # [추가 3] 에러 발생 시 처리 로직 강화
                error_msg = str(e).lower()
                logging.error(f"exception watch_orders: {e}")
                traceback.print_exc()

                # recv_window 또는 timestamp 에러가 발생하면 즉시 시간 동기화 실행
                if "recvwindow" in error_msg or "timestamp" in error_msg:
                    logging.warning("⚠️ Timestamp error detected! Force syncing time...")
                    await self.resync_exchange_time()
                
                await asyncio.sleep(1)

    async def fetch_open_orders(self, symbol: str = None) -> [dict]:
        fetched = None
        open_orders = {}
        limit = 50
        try:
            fetched = await self.cca.fetch_open_orders(symbol=symbol, limit=limit)
            while True:
                if all([elm["id"] in open_orders for elm in fetched]):
                    break
                next_page_cursor = None
                for elm in fetched:
                    elm["position_side"] = determine_pos_side_ccxt(elm)
                    elm["qty"] = elm["amount"]
                    open_orders[elm["id"]] = elm
                    if "nextPageCursor" in elm["info"]:
                        next_page_cursor = elm["info"]["nextPageCursor"]
                if len(fetched) < limit:
                    break
                if next_page_cursor is None:
                    break
                # fetch more
                fetched = await self.cca.fetch_open_orders(
                    symbol=symbol, limit=limit, params={"cursor": next_page_cursor}
                )
            return sorted(open_orders.values(), key=lambda x: x["timestamp"])
        except Exception as e:
            logging.error(f"error fetching open orders {e}")
            print_async_exception(fetched)
            traceback.print_exc()
            return False

    async def fetch_positions(self):
        fetched_positions, fetched_balance = None, None
        positions = {}
        limit = 200
        try:
            fetched_positions, fetched_balance = await asyncio.gather(
                self.cca.fetch_positions(params={"limit": limit, "recv_window": self.recv_window}), self.cca.fetch_balance(params={"recv_window": self.recv_window})
            )
            balinfo = fetched_balance["info"]["result"]["list"][0]
            if balinfo["accountType"] == "UNIFIED":
                balance = float(balinfo["totalWalletBalance"])
                if not hasattr(self, "previous_rounded_balance"):
                    self.previous_rounded_balance = balance
                self.previous_rounded_balance = pbr.hysteresis_rounding(
                    balance,
                    self.previous_rounded_balance,
                    self.hyst_rounding_balance_pct,
                    self.hyst_rounding_balance_h,
                )
                balance = self.previous_rounded_balance
            else:
                balance = fetched_balance[self.quote]["total"]
            while True:
                if all([elm["symbol"] + elm["side"] in positions for elm in fetched_positions]):
                    break
                next_page_cursor = None
                for elm in fetched_positions:
                    elm["position_side"] = determine_pos_side_ccxt(elm)
                    elm["size"] = float(elm["contracts"])
                    elm["price"] = float(elm["entryPrice"])
                    positions[elm["symbol"] + elm["side"]] = elm
                    if "nextPageCursor" in elm["info"]:
                        next_page_cursor = elm["info"]["nextPageCursor"]
                    positions[elm["symbol"] + elm["side"]] = elm
                if len(fetched_positions) < limit:
                    break
                if next_page_cursor is None:
                    break
                # fetch more
                fetched_positions = await self.cca.fetch_positions(
                    params={"cursor": next_page_cursor, "limit": limit, "recvWindow": self.recv_window
                }
                )
            return sorted(positions.values(), key=lambda x: x["timestamp"]), balance
        except Exception as e:
            logging.error(f"error fetching positions and balance {e}")
            print_async_exception(fetched_positions)
            print_async_exception(fetched_balance)
            traceback.print_exc()
            return False

    async def fetch_tickers(self):
        fetched = None
        try:
            fetched = await self.cca.fetch_tickers()
            return fetched
        except Exception as e:
            logging.error(f"error fetching tickers {e}")
            print_async_exception(fetched)
            traceback.print_exc()
            if "bybit does not have market symbol" in str(e):
                # ccxt is raising bad symbol error
                # restart might help...
                raise Exception("ccxt gives bad symbol error... attempting bot restart")
            return False

    async def fetch_ohlcv(self, symbol: str, timeframe="1m"):
        # intervals: 1,3,5,15,30,60,120,240,360,720,D,M,W
        fetched = None
        try:
            fetched = await self.cca.fetch_ohlcv(symbol, timeframe=timeframe, limit=1000)
            return fetched
        except Exception as e:
            logging.error(f"error fetching ohlcv for {symbol} {e}")
            print_async_exception(fetched)
            traceback.print_exc()
            return False

    async def fetch_pnls_sub(
        self,
        start_time: int = None,
        end_time: int = None,
    ):
        if start_time is None:
            pnls = await self.fetch_pnl(start_time=start_time, end_time=end_time)
        else:
            week = 1000 * 60 * 60 * 24 * 7
            pnls = []
            if end_time is None:
                end_time = int(self.get_exchange_time() + 1000 * 60 * 60 * 24)
            # bybit has limit of 7 days per paginated fetch
            # fetch multiple times
            i = 1
            while i < 52:  # limit n fetches to 52 (one year)
                sts = end_time - week * i
                ets = sts + week
                sts = max(sts, start_time)
                fetched = await self.fetch_pnl(start_time=sts, end_time=ets)
                pnls.extend(fetched)
                if sts <= start_time:
                    break
                i += 1
                logging.info(f"fetched pnls for more than a week {ts_to_date_utc(sts)}")
        return sorted(pnls, key=lambda x: x["timestamp"])

    async def fetch_pnl(
        self,
        start_time: int = None,
        end_time: int = None,
        limit: int = None,
    ):
        fetched = None
        all_pnls = []
        ids_seen = set()
        if limit is None:
            limit = 100
        try:
            params = {"category": "linear", "limit": limit}
            if start_time is not None:
                params["startTime"] = int(start_time)
            if end_time is not None:
                params["endTime"] = int(end_time)
            fetched = (await self.cca.private_get_v5_position_closed_pnl(params))["result"]
            while True:
                fetched["list"] = sorted(
                    floatify(fetched["list"]), key=lambda x: float(x["updatedTime"])
                )
                for i in range(len(fetched["list"])):
                    fetched["list"][i]["timestamp"] = float(fetched["list"][i]["updatedTime"])
                    fetched["list"][i]["symbol"] = self.get_symbol_id_inv(
                        fetched["list"][i]["symbol"]
                    )
                    fetched["list"][i]["pnl"] = float(fetched["list"][i]["closedPnl"])
                    fetched["list"][i]["side"] = fetched["list"][i]["side"].lower()
                    fetched["list"][i]["position_side"] = (
                        "long" if fetched["list"][i]["side"] == "sell" else "short"
                    )
                if fetched["list"] == []:
                    break
                if (
                    fetched["list"][0]["orderId"] in ids_seen
                    and fetched["list"][-1]["orderId"] in ids_seen
                ):
                    break
                all_pnls.extend(fetched["list"])
                for elm in fetched["list"]:
                    ids_seen.add(elm["orderId"])
                if start_time is None:
                    break
                if fetched["list"][0]["updatedTime"] <= start_time:
                    break
                if not fetched["nextPageCursor"]:
                    break
                if len(fetched["list"]) < limit:
                    break
                logging.info(
                    f"fetched pnls from {ts_to_date_utc(fetched['list'][-1]['updatedTime'])} n pnls: {len(fetched['list'])}"
                )
                params["cursor"] = fetched["nextPageCursor"]
                fetched = (await self.cca.private_get_v5_position_closed_pnl(params))["result"]
            return sorted(all_pnls, key=lambda x: x["updatedTime"])
        except Exception as e:
            logging.error(f"error fetching pnls {e}")
            print_async_exception(fetched)
            traceback.print_exc()
            return []

    async def fetch_fills(self, start_time, end_time, limit=None):
        # [수정] 재시도 횟수 설정
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if start_time is None:
                    result = await self.cca.fetch_my_trades()
                    return sorted(result, key=lambda x: x["timestamp"])
                
                if end_time is None:
                    end_time = int(self.get_exchange_time() + 1000 * 60 * 60 * 4)
                
                all_fetched_fills = []
                prev_hash = ""
                
                for _ in range(100):
                    fills = await self.cca.fetch_my_trades(
                        limit=limit, params={"paginate": True, "endTime": int(end_time)}
                    )
                    if not fills:
                        break
                    fills.sort(key=lambda x: x["timestamp"])
                    all_fetched_fills += fills
                    if fills[0]["timestamp"] <= start_time:
                        break
                    new_hash = calc_hash([x["id"] for x in fills])
                    if new_hash == prev_hash:
                        break
                    prev_hash = new_hash
                    logging.info(
                        f"fetched fills from {fills[0]['datetime']} to {fills[-1]['datetime']} n fills: {len(fills)}"
                    )
                    end_time = fills[0]["timestamp"]
                    limit = 1000
                else:
                    logging.error(f"more than 100 calls to ccxt fetch_my_trades")
                
                return sorted(all_fetched_fills, key=lambda x: x["timestamp"])

            except Exception as e:
                # [핵심 수정] InvalidNonce(시간 에러) 발생 시 처리
                if "InvalidNonce" in str(e) or "timestamp" in str(e).lower():
                    logging.warning(f"⚠️ Timestamp error in fetch_fills (Attempt {attempt+1}/{max_retries}). Resyncing time...")
                    await self.resync_exchange_time() # 시간 다시 맞춤
                    await asyncio.sleep(1) # 1초 대기 후 재시도
                    continue # 루프의 처음으로 돌아가 재시도
                else:
                    # 다른 에러라면 그냥 에러 발생시키기
                    raise e
        
        return []

    async def fetch_pnls(self, start_time=None, end_time=None, limit=None):
        # fetch fills first, then pnls (bybit has them in separate endpoints)
        if start_time:
            if self.get_exchange_time() - start_time < 1000 * 60 * 60 * 4 and limit == 100:
                # set start time to None (fetch latest) if start time is recent
                start_time = None
        fills = await self.fetch_fills(start_time=start_time, end_time=end_time, limit=limit)
        if start_time:
            fills = [x for x in fills if x["timestamp"] >= start_time - 1000 * 60 * 60]
        if not fills:
            return []
        start_time = fills[0]["timestamp"]
        pnls = await self.fetch_pnls_sub(start_time=start_time, end_time=end_time)

        fillsd = defaultdict(list)
        for x in fills:
            x["orderId"] = x["info"]["orderId"]
            x["position_side"] = self.determine_pos_side(x)
            x["pnl"] = 0.0
            fillsd[x["orderId"]].append(x)
        pnls_ids = set()
        for x in pnls:
            pnls_ids.add(x["orderId"])
            if x["orderId"] in fillsd:
                fillsd[x["orderId"]][-1]["pnl"] = x["pnl"]
            else:
                # logging.info(f"debug missing order id in fills {x['orderId']} {x}")
                x["info"] = {"execId": uuid4().hex}
                x["id"] = x["orderId"]
                fillsd[x["orderId"]] = [x]
        joined = {x["info"]["execId"]: x for x in flatten(fillsd.values())}
        return sorted(joined.values(), key=lambda x: x["timestamp"])

    def determine_pos_side(self, x):
        if x["side"] == "buy":
            return "short" if float(x["info"]["closedSize"]) != 0.0 else "long"
        return "long" if float(x["info"]["closedSize"]) != 0.0 else "short"

    async def execute_cancellation(self, order: dict) -> dict:
        executed = None
        try:
            executed = await self.cca.cancel_order(order["id"], symbol=order["symbol"])
            return executed
        except Exception as e:
            logging.error(f"error cancelling order {order} {e}")
            print_async_exception(executed)
            traceback.print_exc()
            return {}

    async def execute_cancellations(self, orders: [dict]) -> [dict]:
        return await self.execute_multiple(orders, "execute_cancellation")

    async def execute_order(self, order: dict) -> dict:
        executed = await self.cca.create_order(
            type=order["type"] if "type" in order else "limit",
            symbol=order["symbol"],
            side=order["side"],
            amount=abs(order["qty"]),
            price=order["price"],
            params={
                "positionIdx": 1 if order["position_side"] == "long" else 2,
                "timeInForce": (
                    "postOnly" if self.config["live"]["time_in_force"] == "post_only" else "GTC"
                ),
                "orderLinkId": order["custom_id"],
            },
        )
        return executed

    async def execute_orders(self, orders: [dict]) -> [dict]:
        return await self.execute_multiple(orders, "execute_order")

    async def update_exchange_config_by_symbols(self, symbols):
        coros_to_call_lev, coros_to_call_margin_mode = {}, {}
        for symbol in symbols:
            try:
                coros_to_call_margin_mode[symbol] = asyncio.create_task(
                    self.cca.set_margin_mode(
                        "cross",
                        symbol=symbol,
                        params={
                            "leverage": int(self.config_get(["live", "leverage"], symbol=symbol))
                        },
                    )
                )
            except Exception as e:
                logging.error(f"{symbol}: error setting cross mode {e}")
            try:
                coros_to_call_lev[symbol] = asyncio.create_task(
                    self.cca.set_leverage(
                        int(self.config_get(["live", "leverage"], symbol=symbol)), symbol=symbol
                    )
                )
            except Exception as e:
                logging.error(f"{symbol}: a error setting leverage {e}")
        for symbol in symbols:
            res = None
            to_print = ""
            try:
                res = await coros_to_call_lev[symbol]
                to_print += f" set leverage {res} "
            except Exception as e:
                if '"retCode":110043' in e.args[0]:
                    to_print += f" leverage: {e}"
                else:
                    logging.error(f"{symbol} error setting leverage {e}")
            try:
                res = await coros_to_call_margin_mode[symbol]
                to_print += f"set cross mode {res}"
            except Exception as e:
                if '"retCode":110026' in e.args[0]:
                    to_print += f" set cross mode: {res} {e}"
                else:
                    logging.error(f"{symbol} error setting cross mode {res} {e}")
            if to_print:
                logging.info(f"{symbol}: {to_print}")

    async def update_exchange_config(self):
        try:
            res = await self.cca.set_position_mode(True)
            logging.info(f"set hedge mode {res}")
        except Exception as e:
            logging.error(f"error setting hedge mode {e}")

    async def fetch_ohlcvs_1m(self, symbol: str, since: float = None, limit=None):
        n_candles_limit = 1000 if limit is None else limit
        if since is None:
            result = await self.cca.fetch_ohlcv(symbol, timeframe="1m", limit=n_candles_limit)
            return result
        since = since // 60000 * 60000
        max_n_fetches = 5000 // n_candles_limit
        all_fetched = []
        for i in range(max_n_fetches):
            fetched = await self.cca.fetch_ohlcv(
                symbol, timeframe="1m", since=int(since), limit=n_candles_limit
            )
            all_fetched += fetched
            if len(fetched) < n_candles_limit:
                break
            since = fetched[-1][0]
        all_fetched_d = {x[0]: x for x in all_fetched}
        return sorted(all_fetched_d.values(), key=lambda x: x[0])
