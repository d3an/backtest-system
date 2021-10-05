from datetime import date, timedelta
import os
from typing import Callable

import boto3
import pandas as pd

from execution import BacktestBroker
from commission import ib_commission
from stats import StatsHandler
from strategies.reversal import Reversal


class Backtest:
    def __init__(self, start_date: date, end_date: date, commission: Callable[[float, float], float], broker,
                 stats, strategy, cash: float, use_local_data: bool = False, local_data_dir: str = ""):
        self.start_date = start_date
        self.current_date = start_date
        self.end_date = end_date
        self.stats = stats(start_date, cash)
        self.strategy = strategy(start_date, commission, broker, stats, cash)  # commission is a function

        self.use_local_data = use_local_data
        self.local_data_dir = local_data_dir

        if not self.use_local_data:
            self.client = boto3.client("s3")

    def get_data(self):
        if self.use_local_data:
            return self.get_local_data()

        return self.get_s3_data()

    def get_s3_data(self):
        try:
            obj = self.client.get_object(
                Bucket="<BUCKET_NAME>",
                Key=self.current_date.strftime("%Y-%m-%d.csv"),
            )
            return pd.read_csv(obj["Body"])
        except Exception as error:
            print(f"[ERROR] {error}")

    def get_local_data(self):
        try:
            return pd.read_csv(os.path.join(self.local_data_dir, self.current_date.strftime("%Y-%m-%d.csv")))
        except Exception as error:
            print(f"[ERROR] {error}")

    def run(self):
        while self.current_date <= self.end_date:
            print(f'\n{self.current_date.strftime("%Y-%m-%d")}\n')
            # add way to update root Backtest stats with child Strategy stats
            daily_stats = self.strategy.next(self.get_data(), self.current_date)
            # self.stats.update(daily_stats)
            self.current_date += timedelta(days=1)


if __name__ == "__main__":
    start = date(year=2021, month=8, day=1)
    end = date(year=2021, month=8, day=15)
    bt = Backtest(start, end, ib_commission, BacktestBroker, StatsHandler, Reversal,
                  cash=100000.0, use_local_data=True, local_data_dir="data")
    bt.run()
