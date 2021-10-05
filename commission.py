def ib_commission(quantity: float, price: float) -> float:
    return max(0.01 * quantity * price, 1.0)
