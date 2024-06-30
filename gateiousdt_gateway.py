
import hashlib
import hmac
import json
from collections import defaultdict
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock
from time import sleep, time
from typing import Dict, List,Union, Tuple
from urllib.parse import urlencode

from vnpy_rest import Request, RestClient, Response
from vnpy_websocket import WebsocketClient
from vnpy.event import Event,EventEngine
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Interval,
    Offset,
    OrderType,
    Product,
    Status,
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy.trader.utility import (
    round_to, 
    ZoneInfo,
)

# 中国时区
CHINA_TZ = ZoneInfo("Asia/Shanghai")

# 模拟盘合约REST API地址
TESTNET_REST_HOST = "https://fx-api-testnet.gateio.ws"
# 实盘合约REST API地址
REST_HOST = "https://api.gateio.ws" #原地址https://api.gateio.ws,https://fx-api.gateio.ws
# 模拟盘合约WS API地址
TESTNET_WEBSOCKET_HOST = "wss://fx-ws-testnet.gateio.ws/v4/ws"
# 实盘合约WS API地址
WEBSOCKET_HOST = "wss://fx-ws.gateio.ws/v4/ws/usdt"  # usdt本位永续ws_host

INTERVAL_VT2GATEIO: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

# 委托状态映射
STATUS_GATEIO2VT: Dict[str, Status] = {
    "open": Status.NOTTRADED,
    "finished": Status.ALLTRADED,
    "invalid": Status.REJECTED,
    "inactive": Status.SUBMITTING
}

TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}
OPPOSITE_DIRECTION = {
    Direction.LONG: Direction.SHORT,
    Direction.SHORT: Direction.LONG,
}

# 鉴权类型
class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
    API_KEY: int = 2

# 合约数据全局缓存字典
symbol_contract_map: Dict[str, ContractData] = {}


# ----------------------------------------------------------------------------------------------------
class GateioUsdtGateway(BaseGateway):
    """
    * GateioUsdt永续合约
    * 仅支持单向持仓模式,下单合约数量张数(int)
    """
    default_name: str = "GATEIO_USDT"

    default_setting = {
        "key": "",
        "secret": "",
        "服务器": ["REAL", "TESTNET"],
        "代理地址": "",
        "代理端口": 0,
    }
    # 所有合约列表

    exchanges = [Exchange.GATEIO]
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, event_engine: EventEngine, gateway_name: str):
        """ """
        super().__init__(event_engine, gateway_name)

        self.ws_api: GateioUsdtWebsocketApi = GateioUsdtWebsocketApi(self)
        self.rest_api: GateioUsdtRestApi = GateioUsdtRestApi(self)

        self.orders: Dict[str, OrderData] = {}

    # ----------------------------------------------------------------------------------------------------
    def connect(self, log_account: Dict):
        """ """
        key: str= log_account["key"]
        secret: str = log_account["secret"]
        server: str = log_account["服务器"]
        proxy_host: str = log_account["代理地址"]
        proxy_port: int = log_account["代理端口"]

        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)
        self.init_query()
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest):
        """ """
        self.ws_api.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest):
        """ """
        return self.rest_api.send_order(req)
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest):
        """ """
        self.rest_api.cancel_order(req)
    # ----------------------------------------------------------------------------------------------------
    def query_account(self):
        """ """
        self.rest_api.query_account()
    # ----------------------------------------------------------------------------------------------------
    def query_position(self):
        """ """
        self.rest_api.query_position()
    # ----------------------------------------------------------------------------------------------------
    def query_order(self):
        self.rest_api.query_order()
    # ----------------------------------------------------------------------------------------------------
    def query_plan_order(self):
        self.rest_api.query_plan_order()    
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        查询合约历史数据
        """
        return self.rest_api.query_history(req)

    # ----------------------------------------------------------------------------------------------------
    def close(self):
        """ """
        self.rest_api.stop()
        self.ws_api.stop()
    # ----------------------------------------------------------------------------------------------------
    def process_timer_event(self, event: Event):
        """
        轮询账户，持仓，未完成委托单函数
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    def init_query(self):
        """ """
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
    # ----------------------------------------------------------------------------------------------------
    def on_order(self, order: OrderData) -> None:
        """推送委托数据"""
        self.orders[order.orderid] = copy(order)
        super().on_order(order)

        ######在主屏和log中显示order信息
        self.write_log('order.id:{},order.status:{},order.direction:{},order.offset:{},order.price:{},order.volume:{},order.traded:{}'.format(order.vt_orderid,order.status,order.direction,order.offset,order.price,order.volume,order.traded))
        ########################

    def get_order(self, orderid: str) -> OrderData:
        """查询委托数据"""
        return self.orders.get(orderid, None)
    
class GateioUsdtRestApi(RestClient):
    """
    Gateios REST API
    """

    def __init__(self, gateway: GateioUsdtGateway):
        """ """
        super().__init__()

        self.gateway:GateioUsdtGateway = gateway
        self.gateway_name:str = gateway.gateway_name
        self.ws_api:GateioUsdtWebsocketApi = gateway.ws_api

        self.key:str = ""
        self.secret:str = ""
        self.account_id = ""
        self.server = ""
        self.proxy_host = ""
        self.proxy_port = 0

        # 生成委托单号加线程锁
        self.order_count: int = 1_000_000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0
        # 用户自定义委托单id和交易所委托单id映射
        self.orderid_map: Dict[str, str] = defaultdict(str)

    # ----------------------------------------------------------------------------------------------------
    def sign(self, request):
        """
        Generate signature.
        """
        request.headers = generate_sign(self.key, self.secret, request.method, request.path, get_params=request.params, get_data=request.data)

        if not request.data:
            request.data = ""

        return request
    # ----------------------------------------------------------------------------------------------------
    def connect(self, key: str, secret: str, server: str, proxy_host: str, proxy_port: int):
        """
        初始化连接REST
        """
        self.key = key
        self.secret = secret
        self.server = server
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.connect_time = (
            int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )
        if server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()
        self.gateway.write_log(f"交易接口：{self.gateway_name}，REST API连接成功")

        #self.query_time()
        # Query private data after time offset is calculated
        if self.key and self.secret:
            self.query_account()
            self.query_position()
            self.query_order()
            self.query_plan_order()
        self.query_contract()
    # ----------------------------------------------------------------------------------------------------
    def query_time(self) -> None:
        """查询时间"""
        path: str = "/api/v4/spot/time"

        return self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
        )
    
    def set_leverage(self, symbol: str):
        """
        设置单向全仓杠杆
        """
        params = {"leverage": "0", "cross_leverage_limit": "20"}
        self.add_request(method="POST", path=f"/api/v4/futures/usdt/positions/{symbol}/leverage", callback=self.on_leverage, params=params)
    # ----------------------------------------------------------------------------------------------------
    def on_leverage(self, data: dict, request: Request):
        """
        收到修改杠杆回报
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    def query_account(self):
        """ """
        self.add_request(method="GET", path="/api/v4/futures/usdt/accounts", callback=self.on_query_account)
    # ----------------------------------------------------------------------------------------------------
    def query_position(self):
        """ """
        self.add_request(method="GET", path="/api/v4/futures/usdt/positions", callback=self.on_query_position)
    # ----------------------------------------------------------------------------------------------------
    def query_order(self):
        """ """
        params = {
            "status": "open",
        }
        self.add_request(method="GET", path="/api/v4/futures/usdt/orders", callback=self.on_query_order, on_failed=self.query_order_failed, params=params)
    # ----------------------------------------------------------------------------------------------------
    def query_plan_order(self):
        """ """
        params = {
            "status": "open",
        }
        self.add_request(method="GET", path="/api/v4/futures/usdt/price_orders", callback=self.on_query_plan_order, on_failed=self.query_plan_order_failed, params=params)

    # ----------------------------------------------------------------------------------------------------
    def query_contract(self):
        """ """
        self.add_request(method="GET", path="/api/v4/futures/usdt/contracts", callback=self.on_query_contract)
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, req: HistoryRequest):
        """ """
        history: List[BarData] = []
        interval = INTERVAL_VT2GATEIO[req.interval]
        limit: int = 100
        start_time: int = int(datetime.timestamp(req.start))
        time_delta = TIMEDELTA_MAP[req.interval]

        start= req.start
        count = limit
        while True:
            end = start + time_delta * count
            if req.end:
                end_time = int(datetime.timestamp(req.end))
            params = {
                "contract": req.symbol,
                #"from": start,
                "to":str(int(end.timestamp())),
                "interval": interval,
            }
            resp = self.request(method="GET", path="/api/v4/futures/usdt/candlesticks", params=params)
            if resp.status_code // 100 != 2:
                msg = f"标的：{req.vt_symbol}获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data = resp.json()
                if not data:
                    msg = f"标的：{req.vt_symbol}获取历史数据为空"
                    self.gateway.write_log(msg)
                    break
                buf: List[BarData] = []
                for raw in data:
                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=generate_datetime(raw["t"]),
                        interval=req.interval,
                        volume=raw["v"],
                        open_price=float(raw["o"]),
                        high_price=float(raw["h"]),
                        low_price=float(raw["l"]),
                        close_price=float(raw["c"]),
                        gateway_name=self.gateway_name,
                    )
                    # 过滤交易所返回大于等于当前时间的错误bar
                    if bar.datetime >= datetime.now(CHINA_TZ):
                        continue
                    buf.append(bar)
                
                history.extend(buf)
                try:
                    begin: datetime = buf[0].datetime
                    end: datetime = buf[-1].datetime
                    msg: str = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
                    self.gateway.write_log(msg)
                except Exception as e:
                    # 添加异常处理逻辑
                    self.gateway.write_log(f"获取历史数据时发生错误：{e}")
                # 已经获取了所有可用的历史数据或者start_time已经到了请求的终止时间则终止循环
                if len(buf) < count:
                    break

                # 更新开始时间
                if start>req.end:
                    break

                start = bar.datetime
                #start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                #start_time = int(datetime.timestamp(start_dt))

        return history
    # ----------------------------------------------------------------------------------------------------
    def query_order_failed(self, status_code: int, request: Request) -> None:
        """
        查询未成交委托单错误回调
        """
        # 过滤系统错误
        error = request.response.json().get("label", None)
        if error == "SERVER_ERROR":
            return
        self.gateway.write_log(f"错误代码：{status_code}，错误请求：{request.path}，完整请求：{request}")

    # ----------------------------------------------------------------------------------------------------
    def query_plan_order_failed(self, status_code: int, request: Request) -> None:
        """
        查询未成交委托单错误回调
        """
        # 过滤系统错误
        error = request.response.json().get("label", None)
        if error == "SERVER_ERROR":
            return
        self.gateway.write_log(f"错误代码：{status_code}，错误请求：{request.path}，完整请求：{request}")

    # ----------------------------------------------------------------------------------------------------
    def _new_order_id(self) -> int:
        """
        生成本地委托号
        """
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest):
        """ """
        # 生成本地委托号
        orderid: str = req.symbol + "-" + str(self.connect_time + self._new_order_id())

        order = req.create_order_data(orderid, self.gateway_name)
        order.datetime = datetime.now(CHINA_TZ)

        self.gateway.on_order(order)

        if req.direction == Direction.SHORT:
            volume = -int(req.volume)
        else:
            volume = int(req.volume)

        if req.type == OrderType.LIMIT or req.type == OrderType.MARKET:
            if req.type == OrderType.LIMIT:
                request_body = {"contract": req.symbol, "size": volume, "price": str(req.price), "tif": "gtc", "text": f"t-{orderid}"}
            if req.type == OrderType.MARKET:
                request_body = {"contract": req.symbol, "size": volume, "price": "0", "tif": "ioc", "text": f"t-{orderid}"}
            if req.offset == Offset.CLOSE:
                request_body["reduce_only"] = True

            data = json.dumps(request_body)
            #data = request_body

            self.add_request(
                method="POST",
                path="/api/v4/futures/usdt/orders",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_error=self.on_send_order_error,
                on_failed=self.on_send_order_failed,
            )
        if req.type == OrderType.STOP:
            if volume>0:
                rule=1
            else:
                rule=2
            request_body = {
                "initial":{
                    "contract":req.symbol,
                    "size":volume,
                    "price":"0",#表示市价下单
                    "tif": "ioc"
                    },
                "trigger":{
                    "strategy_type":0,
                    "price_type":0,
                    "price":str(req.price),
                    "rule":rule,
                    "expiration":86400
                    }
                }
            if req.offset == Offset.CLOSE:
                request_body["initial"]["close"] = True

            data = json.dumps(request_body)

            self.add_request(
                method="POST",
                path="/api/v4/futures/usdt/price_orders",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_error=self.on_send_order_error,
                on_failed=self.on_send_order_failed,
            )
        
        return order.vt_orderid
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest):
        """ """

        order: OrderData = self.gateway.get_order(req.orderid)
        if order.type==OrderType.STOP:
            typestr="price_orders"
            if "-" in str(req.orderid):
                pathstr=f"/api/v4/futures/usdt/{typestr}/{order.reference}"
            else:
                pathstr=f"/api/v4/futures/usdt/{typestr}/{req.orderid}"
        else:
            typestr="orders"
            if "-" in str(req.orderid):
                pathstr=f"/api/v4/futures/usdt/{typestr}/t-{req.orderid}"
            else:
                pathstr=f"/api/v4/futures/usdt/{typestr}/{req.orderid}"
        self.add_request(
            method="DELETE", path=pathstr, callback=self.on_cancel_order, on_failed=self.on_cancel_order_failed, extra=order
        )
    # ----------------------------------------------------------------------------------------------------
    def on_query_time(self, data: dict, request: Request) -> None:
        """时间查询回报"""
        local_time: int = int(time() * 1000)
        server_time: int = int(data["server_time"])
        self.time_offset: int = local_time - server_time

        self.gateway.write_log(f"服务器时间已更新, 本地时延: {self.time_offset}ms")

    def on_query_account(self, data, request):
        """ """
        self.account_id = str(data["user"])
        account = AccountData(
            accountid=f"USDT_{self.gateway_name}",
            balance=float(data["total"]),
            frozen=float(data["total"]) - float(data["available"]),
            #position_profit=float(data["unrealised_pnl"]),
            #margin=float(data["order_margin"]),
            #datetime=datetime.now(CHINA_TZ),
            gateway_name=self.gateway_name,
        )

        if account.balance:
            self.gateway.on_account(account)

        self.gateway.write_log("账户资金查询成功")

    # ----------------------------------------------------------------------------------------------------
    def on_query_position(self, data, request):
        """ """
        for raw in data:
            volume = float(raw["size"])
            if volume >= 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT
            position = PositionData(
                symbol=raw["contract"],
                exchange=Exchange.GATEIO,
                volume=abs(volume),
                price=float(raw["entry_price"]),
                pnl=float(raw["unrealised_pnl"]),
                direction=direction,
                gateway_name=self.gateway_name,
            )
            if position.volume>0:
                self.gateway.on_position(position)

        self.gateway.write_log("持仓信息查询成功")
    # ----------------------------------------------------------------------------------------------------
    def on_query_order(self, data, request):
        """ """
        for raw in data:
            order_type: OrderType = OrderType.LIMIT

            volume = abs(raw["size"])
            traded = abs(raw["size"] - raw["left"])
            status = get_order_status(raw["status"], volume, traded)
            if raw["size"] > 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT

            order = OrderData(
                orderid=str(raw["id"]),
                symbol=raw["contract"],
                exchange=Exchange.GATEIO,
                price=float(raw["price"]),
                type=order_type,
                volume=volume,
                direction=direction,
                status=status,
                datetime=generate_datetime(raw["create_time"]),
                gateway_name=self.gateway_name,
            )
            reduce_only = raw["is_reduce_only"]
            if reduce_only:
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)
        self.gateway.write_log("普通委托信息查询成功")

    def on_query_plan_order(self, data, request):
        """"""
        for raw in data:
            order_type: OrderType = OrderType.STOP

            volume = abs(raw["initial"]["size"])
            status = get_plan_order_status(raw)
            if raw["initial"]["size"] > 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT

            order = OrderData(
                orderid=str(raw["id"]),
                symbol=raw["initial"]["contract"],
                exchange=Exchange.GATEIO,
                price=float(raw["trigger"]["price"]),
                type=order_type,
                volume=volume,
                direction=direction,
                status=status,
                datetime=generate_datetime(raw["create_time"]),
                gateway_name=self.gateway_name,
            )
            reduce_only = raw["initial"]["is_reduce_only"]
            if reduce_only:
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)
        self.gateway.write_log("计划委托信息查询成功")

    # ----------------------------------------------------------------------------------------------------
    def on_query_contract(self, data, request):
        """ """
        for raw in data:
            symbol = raw["name"]
            contract = ContractData(
                symbol=symbol,
                exchange=Exchange.GATEIO,
                name=symbol,
                pricetick=float(raw["order_price_round"]),#委托价格最小单位
                size=float(raw["quanto_multiplier"]),  # 合约面值，即1张合约对应多少标的币种
                min_volume=raw["order_size_min"],
                product=Product.FUTURES,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=True
            )
            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        self.gateway.write_log(f"交易接口：{self.gateway_name} 合约信息查询成功")

        # 等待rest api获取到account_id再连接websocket api
        self.ws_api.connect(
            self.key,
            self.secret,
            self.server,
            self.proxy_host,
            self.proxy_port,
            self.account_id,
        )
    # ----------------------------------------------------------------------------------------------------
    def on_send_order(self, data, request):
        """ """
        res=json.loads(request.response.text)
        if "trigger" in res:#计划委托
            order=self.gateway.get_order(request.extra.orderid)
            order.reference=str(res["id"])

    # ----------------------------------------------------------------------------------------------------
    def on_send_order_failed(self, status_code: int, request: Request):
        """
        Callback when sending order failed on server.
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        order.reference=request.response.text
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request):
        """
        Callback when sending order caused exception.
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_order(self, data, request):
        """ """
        if data["status"] == "error":
            error_code = data["err_code"]
            error_msg = data["err_msg"]
            self.gateway.write_log(f"撤单失败，错误代码：{error_code}，信息：{error_msg}")
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_order_failed(self, status_code: str, request: Request):
        """
        Callback when canceling order failed on server.
        """
        if request.extra:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)

        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)


# ----------------------------------------------------------------------------------------------------
class GateioUsdtWebsocketApi(WebsocketClient):
    """ """

    def __init__(self, gateway: GateioUsdtGateway):
        """ """
        super(GateioUsdtWebsocketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.key = ""
        self.secret = ""
        self.account_id = ""

        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}
        self.order_book_bids = defaultdict(dict)  # 订单簿买单字典
        self.order_book_asks = defaultdict(dict)  # 订单簿卖单字典
        self.topic_map = {
            "futures.order_book_update":self.on_depth,
            "futures.book_ticker":self.on_book_ticker,
            "futures.trades":self.on_public_trade,
            "futures.orders": self.on_order,
            "futures.autoorders": self.on_plan_orders,
            #"futures.usertrades": self.on_trade,
            "futures.positions": self.on_position,
        }
    # ----------------------------------------------------------------------------------------------------
    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
        account_id: str,
    ):
        """ """
        self.key = key
        self.secret = secret
        self.account_id = account_id

        if server == "REAL":
            self.init(WEBSOCKET_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_WEBSOCKET_HOST, proxy_host, proxy_port)

        self.start()
    # ----------------------------------------------------------------------------------------------------
    def on_connected(self):
        """ """
        self.gateway.write_log(f"交易接口：{self.gateway_name} Websocket API连接成功")
        # 重订阅标的tick数据
        for req in list(self.subscribed.values()):
            self.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest):
        """
        订阅tick数据
        """
        while not self.account_id:
            rest_api = self.gateway.rest_api
            rest_api.query_account()
            self.account_id = rest_api.account_id
            sleep(0.5)

        if req.symbol not in symbol_contract_map:
            self.gateway.write_log(f"找不到该合约代码{req.symbol}")
            return

        # 订阅用户订单信息
        topic = [
            #"futures.usertrades",
            "futures.orders",
            "futures.autoorders",
            "futures.positions",
        ]
        for channel in topic:
            topic_req = self.generate_req(channel=channel, event="subscribe", pay_load=[self.account_id, req.symbol])
            self.send_packet(topic_req)
        tick = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            name=req.symbol,
            datetime=datetime.now(CHINA_TZ),
            gateway_name=self.gateway_name,
        )
        self.ticks[req.symbol] = tick
        self.subscribed[req.symbol] = req
        # 订阅tick行情
        tick_req = self.generate_req(channel="futures.tickers", event="subscribe", pay_load=[req.symbol])
        self.send_packet(tick_req)
        # 20ms深度推送只支持20档位
        depth_req = self.generate_req(channel="futures.order_book_update", event="subscribe", pay_load=[req.symbol, "20ms", "20"])
        self.send_packet(depth_req)
        # 订阅逐笔成交
        public_trade_req = self.generate_req(channel="futures.trades", event="subscribe", pay_load=[req.symbol])
        self.send_packet(public_trade_req)
        # 订阅逐笔一档深度
        book_ticker_req = self.generate_req(channel="futures.book_ticker", event="subscribe", pay_load=[req.symbol])
        self.send_packet(book_ticker_req)
    # ----------------------------------------------------------------------------------------------------
    def on_disconnected(self):
        """ """
        self.gateway.write_log(f"交易接口：{self.gateway_name} Websocket API连接断开")
    # ----------------------------------------------------------------------------------------------------
    def on_packet(self, packet: Dict):
        """ """
        timestamp = packet["time_ms"]
        channel = packet["channel"]
        event = packet["event"]
        result = packet["result"]
        error = packet.get("error", None)
        if error:
            self.gateway.write_log(f"交易接口：{self.gateway_name} Websocket API报错：{error}")
            return
        if event == "subscribe":
            return
        if event == "update":
            if channel == "futures.tickers":
                self.on_tick(result, timestamp)
            else:
                self.topic_map[channel](result)
        if channel == "futures.orders" or channel == "futures.positions" or channel == "futures.autoorders":
            ######在日志显示所有ws包
            self.gateway.write_log(packet)
            ########################
        

    # ----------------------------------------------------------------------------------------------------
    def on_error(self, exception_type: type, exception_value: Exception, tb):
        """ """
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def generate_req(self, channel: str, event: str, pay_load: List):
        """ """
        expires = int(time())
        signature = generate_websocket_sign(self.secret, channel, event, expires)

        req = {"time": expires, "channel": channel, "event": event, "payload": pay_load, "auth": {"method": "api_key", "KEY": self.key, "SIGN": signature}}

        return req
    # ----------------------------------------------------------------------------------------------------
    def on_tick(self, raw: List, timestamp: int):
        """
        收到tick回报
        """
        for data in raw:
            symbol = data["contract"]
            tick = self.ticks[symbol]
            tick.high_price = float(data["high_24h"])
            tick.low_price = float(data["low_24h"])
            tick.last_price = float(data["last"])
            tick.volume = int(data["volume_24h_base"])     # 最近24小时币的成交量
            tick.datetime = generate_datetime_ms(timestamp)
    # ----------------------------------------------------------------------------------------------------
    def on_depth(self, raw: Dict):
        """
        收到tick深度回报
        """
        timestamp = raw["t"]
        symbol = raw["s"]
        tick = self.ticks[symbol]
                
        # 更新买单和卖单
        update_order_book(self.order_book_bids[symbol], raw["b"])
        update_order_book(self.order_book_asks[symbol], raw["a"])

        # 设置最高的5个买单和卖单价格及数量
        def set_top_prices_volumes(order_book:Dict[float,float], prefix:str):
            sorted_items = sorted(order_book.items(), key=lambda x: x[0], reverse=(prefix == "bid"))[:5]
            for index, (price, volume) in enumerate(sorted_items, start=1):
                setattr(tick, f"{prefix}_price_{index}", price)
                setattr(tick, f"{prefix}_volume_{index}", volume)
        
        set_top_prices_volumes(self.order_book_bids[symbol], "bid")
        set_top_prices_volumes(self.order_book_asks[symbol], "ask")
        
        # 更新时间
        tick.datetime = generate_datetime_ms(timestamp)
    # ----------------------------------------------------------------------------------------------------
    def on_book_ticker(self, raw: Dict):
        """
        收到逐笔一档深度回报
        """
        symbol = raw["s"]
        timestamp = raw["t"]
        tick = self.ticks[symbol]
        tick.datetime = generate_datetime_ms(timestamp)
        tick.bid_price_1,tick.bid_volume_1 = float(raw["b"]),float(raw["B"])
        tick.ask_price_1,tick.ask_volume_1 = float(raw["a"]),float(raw["A"])
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_public_trade(self, raw: Dict):
        """
        逐笔成交回报
        """
        data = raw[-1]
        symbol = data["contract"]
        timestamp = data["create_time_ms"]
        tick = self.ticks[symbol]
        tick.datetime = generate_datetime_ms(timestamp)
        tick.last_price = float(data["price"])
        self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_order(self, raw: List):
        """
        收到委托单回报
        """
        for data in raw:
            if data["text"].startswith("ao"):#'ao-75605130'ao开头是计划委托单，在on_plan_order处理，不需要在这里处理
                continue

            if data["size"] > 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT

            volume = abs(float(data["size"]))

            traded = abs(float(data["size"]) - float(data["left"]))
            status = get_order_status(data["status"], volume, traded)
            reduce_only = data["is_reduce_only"]
            order = OrderData(
                orderid=str(data["text"][2:]),
                symbol=data["contract"],
                exchange=Exchange.GATEIO,
                price=float(data["price"]),
                volume=volume,
                traded=traded,
                type=OrderType.LIMIT,
                direction=direction,
                status=status,
                datetime=generate_datetime(data["create_time"]),
                gateway_name=self.gateway_name,
            )
            if reduce_only:
                order.offset = Offset.CLOSE
            else:
                order.offset = Offset.OPEN

            self.gateway.on_order(order)

            # 将成交数量四舍五入到正确精度
            trade_volume: float = abs(float(data["size"]))
            contract: ContractData = symbol_contract_map.get(order.symbol, None)
            if contract:
                trade_volume = round_to(trade_volume, contract.min_volume)

            if status!=Status.ALLTRADED:
                continue

            trade: TradeData = TradeData(
                symbol=order.symbol,
                exchange=order.exchange,
                orderid=order.orderid,
                tradeid="trade-"+str(data["id"]),
                direction=order.direction,
                price=float(data["price"]),
                volume=trade_volume,
                datetime=generate_datetime(data["finish_time"]),
                gateway_name=self.gateway_name,
                offset=order.offset
            )
            self.gateway.on_trade(trade)

    def on_plan_orders(self, raw: List):
        """
        收到计划委托单回报
        """
        for data in raw:
            id=str(data["id"])
            found = False
            for orderid_str in self.gateway.orders:
                if self.gateway.orders[orderid_str].reference==id:
                    orderid=orderid_str
                    found = True
                    break
            if not found:
                orderid=id
            if data["initial"]["size"] > 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT

            volume = abs(float(data["initial"]["size"]))
            reduce_only = data["initial"]["is_reduce_only"]
            status = get_plan_order_status(data)
            if data["status"]=="finished" and data["finish_as"]=="succeeded":
                traded=volume
            else:
                traded = 0
            if data["finish_time"]>0:
                ordertime = generate_datetime(data["finish_time"])
            else:
                ordertime = generate_datetime(data["create_time"])
            order = OrderData(
                orderid=orderid,
                symbol=data["initial"]["contract"],
                exchange=Exchange.GATEIO,
                price=float(data["trigger"]["price"]),
                volume=volume,
                traded=traded,
                type=OrderType.STOP,
                direction=direction,
                status=status,
                datetime=ordertime,
                gateway_name=self.gateway_name,
            )
            if found==True:
                order.reference=str(data["id"])
            if reduce_only:
                order.offset = Offset.CLOSE
            else:
                order.offset = Offset.OPEN

            self.gateway.on_order(order)

            # 将成交数量四舍五入到正确精度
            trade_volume: float = abs(float(data["initial"]["size"]))
            contract: ContractData = symbol_contract_map.get(order.symbol, None)
            if contract:
                trade_volume = round_to(trade_volume, contract.min_volume)

            if status!=Status.ALLTRADED:
                continue

            trade: TradeData = TradeData(
                symbol=order.symbol,
                exchange=order.exchange,
                orderid=order.orderid,
                tradeid=data["trade_id"],
                direction=order.direction,
                price=float(data["trigger"]["price"]),
                volume=trade_volume,
                datetime=generate_datetime(data["finish_time"]),
                gateway_name=self.gateway_name,
                offset=order.offset
            )
            self.gateway.on_trade(trade)
            
    # ----------------------------------------------------------------------------------------------------
    def on_trade(self, raw: List):
        """
        收到成交回报(应该不生效)
        """
        for data in raw:
            volume = float(data["size"])
            if volume > 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT
            trade = TradeData(
                symbol=data["contract"],
                exchange=Exchange.GATEIO,
                orderid=data["text"][2:],
                tradeid=str(data["id"]),
                direction=direction,
                price=float(data["price"]),
                volume=abs(data["size"]),
                datetime=generate_datetime_ms(data["create_time_ms"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)
    # ----------------------------------------------------------------------------------------------------
    def on_position(self, raw: List):
        """
        * 收到持仓回报
        * websocket没有未结持仓盈亏参数
        """
        for data in raw:
            volume = float(data["size"])
            if volume >= 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT
            position = PositionData(
                symbol=data["contract"],
                exchange=Exchange.GATEIO,
                volume=abs(volume),
                price=float(data["entry_price"]),
                pnl=float(data["history_pnl"]),
                direction=direction,
                gateway_name=self.gateway_name,
            )
            if position.volume>0:
                self.gateway.on_position(position)


# ----------------------------------------------------------------------------------------------------
def generate_sign(key:str, secret:str, method, path, get_params=None, get_data=None):
    """ """
    if get_params:
        params = urlencode(get_params)
    else:
        params = ""

    hashed_data = get_hashed_data(get_data)

    timestamp = str(time())

    pay_load = [method, path, params, hashed_data, timestamp]
    pay_load = "\n".join(pay_load)

    signature = hmac.new(secret.encode('utf-8'), pay_load.encode("utf-8"), hashlib.sha512).hexdigest()
    

    headers = {"Accept": "application/json", "Content-Type": "application/json", "KEY": key, "Timestamp": timestamp, "SIGN": signature}

    return headers


# ----------------------------------------------------------------------------------------------------
def get_hashed_data(get_data:str):
    """ """
    hashed_data = hashlib.sha512()
    if get_data:
        data = get_data
        hashed_data.update(data.encode('utf-8'))

    return hashed_data.hexdigest()


# ----------------------------------------------------------------------------------------------------
def generate_websocket_sign(secret: str, channel: str, event: str, time: int):
    """ """
    message = "channel={}&event={}&time={}".format(channel, event, time)

    signature = hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha512).hexdigest()

    return signature


# ----------------------------------------------------------------------------------------------------
def get_order_status(status: str, volume: int, traded: int):
    """
    获取委托单成交状态
    """
    if status == "open":
        if traded:
            return Status.PARTTRADED
        else:
            return Status.NOTTRADED
    else:
        if traded == volume:
            return Status.ALLTRADED
        else:
            return Status.CANCELLED
def get_plan_order_status(raw:List):
    status=raw["status"]
    if status == "open":
        return Status.NOTTRADED
    elif status == "finished":
        finish_as=raw["finish_as"]
        if finish_as == "cancelled":
            return Status.CANCELLED
        if finish_as == "succeeded":
            return Status.ALLTRADED
        if finish_as == "failed":
            return Status.REJECTED
        if finish_as == "expired":
            return Status.REJECTED
    elif status == "invalid":
        return Status.REJECTED
    else:
        return Status.NOTTRADED
# 更新order book的通用函数
def update_order_book(order_book:Dict[float,float], data:List[Dict[str,Union[str,float]]]):
    for item in data:
        price, volume = float(item["p"]), float(item["s"])
        if volume > 0:
            order_book[price] = volume
        else:
            order_book.pop(price, None)
# ----------------------------------------------------------------------------------------------------
def get_order_type(order_type_str: str) -> OrderType:
    """
    close-long-order 委托单止盈止损，平做多仓
    close-short-order 委托单止盈止损，平做空仓
    close-long-position 仓位止盈止损，平多仓
    close-short-position 仓位止盈止损，平空仓
    plan-close-long-position 仓位计划止盈止损，平多仓
    plan-close-short-position 仓位计划止盈止
    """
    pass
def generate_datetime_ms(timestamp: float) -> datetime:
    """生成时间"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    dt: datetime = dt.replace(tzinfo=CHINA_TZ)
    return dt
def generate_datetime(timestamp: float) -> datetime:
    """生成时间"""
    dt: datetime = datetime.fromtimestamp(timestamp)
    dt: datetime = dt.replace(tzinfo=CHINA_TZ)
    return dt