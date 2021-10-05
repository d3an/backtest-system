from datetime import date, datetime
from typing import Callable, List

import pandas as pd

from execution import Action, ExecutionHandler, MarketOrder, Order
from stats import StatsHandler


class Reversal:
    def __init__(self, start_date: date, commission: Callable[[float, float], float],
                 broker, stats, cash: float = 100000.0):
        pd.set_option('max_columns', 10)
        pd.set_option('display.width', 200)
        self.watchlist: pd.DataFrame = pd.DataFrame()
        self.portfolio: pd.DataFrame = pd.DataFrame(columns=[
            "Ticker", "Price", "Volume", "Quantity", "Cost", "Market Value", "Gain ($)", "Gain (%)"
        ]).set_index("Ticker")
        self.stats: StatsHandler = stats(start_date, cash)  # position weight?

        self.max_watchlist_days: int = 6

        self.broker: ExecutionHandler = broker(commission)  # pass reference to portfolio if possible + commission
        self.data: pd.DataFrame = pd.DataFrame()
        self.current_date: date = None

        # put this in stats
        self.cash = cash
        self.position_weight = 0.1

    @staticmethod
    def clean_df(data: pd.DataFrame) -> pd.DataFrame:
        def format_earnings_datetime(x: str):
            if pd.isna(x):
                return x
            elif x[-1] == "b":
                return datetime.strptime(f"{x[:-2]}, 2021 08:30AM", "%b %d, %Y %I:%M%p")
            elif x[-1] == "a":
                return datetime.strptime(f"{x[:-2]}, 2021 04:00PM", "%b %d, %Y %I:%M%p")
            try:
                return datetime.strptime(f"{x}, 2021 04:00PM", "%d-%b, %Y %I:%M%p")
            except ValueError:
                return datetime.strptime(f"{x}, 2021 04:00PM", "%b %d, %Y %I:%M%p")

        data["IPO Date"] = pd.to_datetime(data["IPO Date"])
        data["Earnings"] = data["Earnings"].apply(format_earnings_datetime)
        return data.drop(columns=["No."]).reset_index(drop=True).set_index("Ticker")

    def next(self, data: pd.DataFrame, current_date: date):
        self.current_date = current_date
        self.data = self.clean_df(data)
        self.update_portfolio()
        self.update_watchlist()

        executed_orders = self.broker.execute_orders(
            self.data, self.current_date, self.stats.equity_curve["Cash"].iloc[-1])
        self.process_orders(executed_orders)

        self.exit_predicate()
        self.entry_predicate()

        return self.stats.daily_stats()

    def process_orders(self, executed_orders: List[Order]):
        for order in executed_orders:
            if order.ticker in self.watchlist.index:
                self.watchlist.drop([order.ticker], inplace=True)

            if order.ticker in self.portfolio.index:
                if order.action == Action.SELL:
                    self.portfolio.loc[order.ticker, "Price"] = order.fill_price
                    self.portfolio.loc[order.ticker, "Quantity"] -= order.quantity
                    if self.portfolio.loc[order.ticker, "Quantity"] == 0:
                        self.portfolio.drop([order.ticker], inplace=True)
                elif order.action == Action.BUY:
                    self.portfolio.loc[order.ticker, "Cost"] = (
                            (self.portfolio.loc[order.ticker, "Price"] / self.portfolio.loc[order.ticker, "Quantity"]) +
                            (order.fill_price / order.quantity))
                    self.portfolio.loc[order.ticker, "Price"] = order.fill_price
                    self.portfolio.loc[order.ticker, "Quantity"] += order.quantity
            else:
                self.portfolio.loc[order.ticker] = pd.Series(data={
                    "Price": order.fill_price,
                    "Quantity": order.quantity,
                    "Cost": order.fill_price,
                    "Market Value": order.quantity * order.fill_price,
                })
                self.portfolio.sort_values("Ticker", inplace=True)

            self.stats.update_on_order(self.current_date, order)

        self.portfolio.loc["Market Value"] = self.portfolio["Price"] * self.portfolio["Quantity"]
        self.portfolio.loc["Gain ($)"] = (
            self.portfolio["Market Value"] - (self.portfolio["Quantity"] * self.portfolio["Cost"])
        )
        self.portfolio["Gain (%)"] = (self.portfolio["Price"] - self.portfolio["Cost"]) / self.portfolio["Cost"] * 100

    def run_screen(self):
        results = self.data.loc[
            (self.data["Market Cap"] > 200000) &
            (self.data["Perf Year"] < 0) &
            (self.data["P/E"] > 5)
        ].copy()
        results["Date Added"] = self.current_date

        if len(self.watchlist.index) == 0:
            self.watchlist = results
        else:
            for index, row in results.iterrows():
                if index not in self.watchlist.index:
                    self.watchlist.loc[index] = row
        self.watchlist.sort_values("Ticker", inplace=True)

    def update_watchlist(self):
        if len(self.watchlist.index) == 0:
            return

        self.watchlist.update(self.data)

        # Drop expired items
        self.watchlist.drop(self.watchlist.loc[
            pd.to_datetime(self.watchlist["Date Added"]) + pd.offsets.Day(self.max_watchlist_days)
            <= pd.Timestamp(self.current_date)
        ].index, inplace=True)

    def update_portfolio(self):
        if len(self.portfolio.index) == 0:
            return

        self.portfolio.update(self.data)

        self.portfolio["Market Value"] = self.portfolio["Quantity"] * self.portfolio["Price"]
        self.portfolio["Gain ($)"] = self.portfolio["Market Value"] - (
                    self.portfolio["Quantity"] * self.portfolio["Cost"])
        self.portfolio["Gain (%)"] = (self.portfolio["Price"] - self.portfolio["Cost"]) / self.portfolio["Cost"] * 100

    def entry_predicate(self):
        self.run_screen()

        if len(self.watchlist.index) > 0:
            raw_buys = self.watchlist.loc[self.watchlist["Volume"] > 1000000].copy()

            for index, row in raw_buys.iterrows():
                # create quantity calculation functions in the execution system
                # lets have a "class" of functions for quantity, commission, etc.
                # quantity = np.floor(self.cash * self.position_weight / buy_count / current_cost ))
                self.broker.place_order(
                    MarketOrder(dt=self.current_date, action=Action.BUY, ticker=str(index), quantity=100)
                )

    def exit_predicate(self):
        if len(self.portfolio.index) > 0:
            for index, row in self.portfolio.iterrows():
                # other sell variables
                if self.data.at[index, "Perf Week"] >= 0.1 or self.data.at[index, "Perf Week"] <= -0.1:
                    self.broker.place_order(
                        MarketOrder(self.current_date, Action.SELL, str(index), row["Quantity"])
                    )

        # Place cancel orders if necessary
