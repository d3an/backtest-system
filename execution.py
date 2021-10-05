from abc import ABC, ABCMeta, abstractmethod
from datetime import date
from enum import Enum
from typing import Callable, List

import pandas as pd


class Action(Enum):
    SELL = 1
    BUY = 2


class Status(Enum):
    OPEN = 1
    COMPLETE = 2
    CANCELLED = 3


# TimeInForce provides options that define the length of time over which an order will continue working before cancelled
class TimeInForce(Enum):
    # Default, order cancelled if not executed by end of trading day
    # Unless specified, every order is a Day order
    DAY = 1
    # Good-Til-Cancelled remains in the system until executed or is cancelled by the customer
    # Limit, Stop, and Stop-Limit orders
    GTC = 2
    # Immediate-Or-Cancel order cancels any part of the order that was not filled as soon as the market allowed it
    # Market or Limit orders
    IOC = 3
    # Fill-Or-Kill order specifies that if the entire order cannot be filled as soon as the market allows it, the entire
    # order is cancelled.
    FOK = 4


class Order:
    def __init__(self, dt: date, action: Action, ticker: str, quantity: int):
        """ Initializes an order """
        self.initiated_at = dt
        self.executed_at = None
        self.action = action
        self.status = Status.OPEN

        self.ticker = ticker
        self.quantity = quantity

        self.fill_price = None
        self.commission_paid = 0.0


class MarketOrder(Order):
    def __init__(self, dt: date, action: Action, ticker: str, quantity: int):
        super().__init__(dt, action, ticker, quantity)


class LimitOrder(Order):
    def __init__(self, dt: date, action: Action, ticker: str, quantity: int,
                 limit_price: float, time_in_force: TimeInForce):
        super().__init__(dt, action, ticker, quantity)
        self.limit_price = limit_price
        self.time_in_force = time_in_force


class StopLimitOrder(Order):
    def __init__(self, dt: date, action: Action, ticker: str, quantity: int,
                 stop_price: float, limit_price: float, time_in_force: TimeInForce):
        super().__init__(dt, action, ticker, quantity)
        self.stop_price = stop_price
        self.limit_price = limit_price
        self.time_in_force = time_in_force


class Position:
    def __init__(self, order: Order):
        if isinstance(order, (MarketOrder, LimitOrder)) and order.status == Status.COMPLETE:
            self.ticker = order.ticker
            self.quantity = order.quantity
            self.market_value = order.quantity * order.fill_price
            # self.avg_purchase_price = order.fill_price
            # self.last = order.fill_price
            self.change_in_investment = 0.0
            self.daily_change_in_investment = 0.0


class ExecutionHandler(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abstractmethod
    def execute_orders(self, data: pd.DataFrame, current_date: date, cash: float) -> List[Order]:
        raise NotImplementedError

    @abstractmethod
    def place_order(self, order):
        raise NotImplementedError


class BacktestBroker(ExecutionHandler, ABC):
    def __init__(self, commission: Callable[[float, float], float]):
        self.order_book = {
            Action.BUY: {
                Status.OPEN: [],
                Status.COMPLETE: [],
                Status.CANCELLED: [],
            },
            Action.SELL: {
                Status.OPEN: [],
                Status.COMPLETE: [],
                Status.CANCELLED: [],
            },
        }

        self.commission = commission

    @staticmethod
    def is_business_day(dt: date) -> bool:
        return bool(len(pd.bdate_range(dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d"))))

    def verify_funds(self, quantity: int, fill_price: float, cash: float):
        return quantity * fill_price + self.commission(quantity, fill_price) <= cash

    def execute_orders(self, data: pd.DataFrame, current_date: date, cash: float) -> List[Order]:
        # Verify current_date is a trading day
        if not self.is_business_day(current_date):
            return []

        executed_orders = []

        # Process open sell & buy orders
        for action in Action:
            for order in self.order_book[action][Status.OPEN].copy():
                # should this throw an error?
                if order.initiated_at >= current_date:
                    continue

                close_price = data.loc[order.ticker, "Price"]
                open_price = close_price / (1 + data.loc[order.ticker, "from Open"])
                # prev_close_price = close_price/(1 + data.loc[order.ticker, "Change"])

                # Process orders
                if isinstance(order, MarketOrder) and self.verify_funds(order.quantity, open_price, cash):
                    self.order_book[action][Status.OPEN].remove(order)
                    order.fill_price = open_price
                elif isinstance(order, LimitOrder) and self.verify_funds(
                        order.quantity, order.limit_price, cash) and open_price <= order.limit_price <= close_price:
                    self.order_book[action][Status.OPEN].remove(order)
                    order.fill_price = order.limit_price
                else:
                    continue

                order.executed_at = current_date
                order.status = Status.COMPLETE
                order.commission_paid = self.commission(order.quantity, order.fill_price)
                self.order_book[action][Status.COMPLETE].append(order)
                executed_orders.append(order)

        return executed_orders

    def place_order(self, order: Order):
        self.order_book[order.action][Status.OPEN].append(order)
        # verify order details
