from dataclasses import dataclass
from typing import Optional
import random
import pandas as pd

CONSTANT_PLACE = 30 
data = pd.read_csv("result.csv", skipinitialspace = True, low_memory=False)     
    
def ticker_fill(x):
    exchange_ts = data['exchange_ts'][x]
    bid_price = data['price_BID'][x]
    ask_price = data['price_ASK'][x]
    bid_size = data['size_BID'][x]
    ask_size = data['size_ASK'][x]
    
    return [OrderbookSnapshotUpdate(exchange_ts,list((ask_price, ask_size)),list((bid_price, bid_size))),
            AnonTradeBA(exchange_ts, [AnonTrade(ask_price, ask_size), AnonTrade(bid_price, bid_size)])]

@dataclass
class Order:  # Our own placed order
    order_id: int
    side: str
    size: float
    price: float
    life_time: int
    placed: bool
        
        
@dataclass
class AnonTrade:  # Market trade
    price: str
    size: float

@dataclass
class AnonTradeBA:
    timestamp: int
    bid_ask_trades: list[AnonTrade, AnonTrade]
    
    
@dataclass
class OwnTrade:  # Execution of own placed order
    timestamp: float
    trade_id: int
    order_id: int
    side: str
    size: float
    price: float

        
@dataclass
class OrderbookSnapshotUpdate:  # Orderbook tick snapshot
    timestamp: float
    asks: list[tuple[float, float]]  # tuple[price, size]
    bids: list[tuple[float, float]]


@dataclass
class MdUpdate:  # Data of a tick
    orderbook: Optional[list[OrderbookSnapshotUpdate]] = None
    trades: Optional[list[AnonTradeBA]] = None


class Strategy:
    def __init__(self) -> None:
        pass

    def run(self, sim: "Sim"):
        i = 0
        while True:
            try:
                md_update = sim.tick()
                place = random.randint(0, 100)
                if place < CONSTANT_PLACE:
                    if md_update.trades[-1].bid_ask_trades[0].price != 0:
                        sim.place_order(Order(i, "BID", 0.02, md_update.orderbook[-1].asks[0], 0, 0))
                        i+=1
                else:
                    if md_update.trades[-1].bid_ask_trades[1].price != 0:
                        sim.place_order(Order(i, "ASK", 0.02, md_update.orderbook[-1].bids[0], 0, 0))
                        i+=1

            except StopIteration:
                break
        return sim.trade_list
                

def load_md_from_file(tick_size, trading_time) -> list[MdUpdate]:
    md_new = []
    for i in range(trading_time):
        x = list(map(lambda x: ticker_fill(x), range(tick_size * i, tick_size * (i+1))))
        
        md_new.append(MdUpdate([temp[0] for temp in x], [temp[1] for temp in x]))
    return md_new


class Sim:
    def __init__(self, execution_latency: int, closing_time:int, tick_size: int, trading_time: int, max_pos: int) -> None:
        self.md = iter(load_md_from_file(tick_size, trading_time))
        self.order_list = []
        self.trade_list = []
        self.trade_id = 0
        self.pos = 0
        self.max_pos = max_pos
        self.latency = execution_latency
        self.closing_time = closing_time

    def tick(self) -> MdUpdate:
        for order in self.order_list:
            order.life_time += 1
        self.execute_orders()
        self.prepare_orders()

        return next(self.md)
    
    def prepare_orders(self):
        for order in self.order_list:
            if order.life_time >= self.closing_time:
                self.cancel_order(order)
            if order.life_time >= self.latency and order.placed == 0:
                order.placed = 1


    def execute_orders(self):
        for order in self.order_list:
            if order.placed:
                for trades in next(self.md).trades:
                    if order.side == "BID":
                        if trades.bid_ask_trades[0].price >= order.price and order.size <= trades.bid_ask_trades[0].size and self.pos < self.max_pos:
                            self.trade_list.append(OwnTrade(trades.timestamp, self.trade_id, order.order_id, order.side, order.size, order.price))
                            self.cancel_order(order)
                            self.pos += 1
                            self.trade_id += 1
                            break
                    elif order.side == "ASK":
                        if trades.bid_ask_trades[1].price <= order.price and order.size <= trades.bid_ask_trades[1].size and self.pos > -self.max_pos:
                            self.trade_list.append(OwnTrade(trades.timestamp, self.trade_id, order.order_id, order.side, order.size, order.price))
                            self.cancel_order(order)
                            self.pos -= 1
                            self.trade_id += 1
                            break

                            
    def place_order(self, order: Order):
        self.order_list.append(order)

    def cancel_order(self, order: Order):
        if order in self.order_list:
            self.order_list.remove(order)



strategy = Strategy()
sim = Sim(10, 100, 250, 5000, 10)
trades = strategy.run(sim)

ask = []
bid = []
for trade in trades:
    print(trade.side, trade.price)
    if trade.side == 'BID':
        bid.append(trade.price)
    else:
        ask.append(trade.price)
print(sum(ask), len(ask), sum(bid), len(bid))
