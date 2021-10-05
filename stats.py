from datetime import date

import pandas as pd

from execution import Action, Order


class StatsHandler:
    def __init__(self, start_date: date, cash: float = 100000.0):
        self.equity_curve = pd.DataFrame(data={
            "Date": start_date,
            "Cash": cash,
            "Equity": 0.0,
            "Total Value": cash,
            "Commission Paid": 0.0,
        }, index=["Date"])
        # self.equity_curve = pd.DataFrame(columns=[
        #     "Date", "Cash", "Equity", "Total Value"
        #     # "Commission", "Sharpe Ratio", "Max Drawdown", "Drawdown Duration"
        # ]).set_index("Date")
        # Equity curve: a dataframe ordered by date that records a portfolio's stats on a daily basis
        # -> cash, sharpe ratio, drawdowns, returns, daily ROI,

        # Sharpe ratio
        # Max drawdown
        # Drawdown duration
        # Total return
        # PnL
        pass

    def update_on_order(self, current_date: date, order: Order):
        updated_cash = self.equity_curve["Cash"].iloc[-1] - order.commission_paid
        updated_commission = self.equity_curve["Commission Paid"].iloc[-1] + order.commission_paid
        updated_equity = self.equity_curve["Equity"].iloc[-1]
        if order.action == Action.SELL:
            updated_cash += order.quantity * order.fill_price
            updated_equity -= order.quantity * order.fill_price
        elif order.action == Action.BUY:
            updated_cash -= order.quantity * order.fill_price
            updated_equity += order.quantity * order.fill_price

        self.equity_curve.loc[current_date] = pd.Series(data={
            "Date": current_date,
            "Cash": updated_cash,
            "Equity": updated_equity,
            "Total Value": updated_cash + updated_equity,
            "Commission Paid": updated_commission,
        })

    def daily_stats(self):
        """ Return latest series """
        return self.equity_curve.iloc[-1]
