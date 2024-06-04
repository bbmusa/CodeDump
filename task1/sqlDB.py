import sqlite3
from pydantic import BaseModel

class Order(BaseModel):
    ticker: str
    type: str
    strike: int
    price: float
    ordertime: str


class sqlDb:
    def __init__(self):
        connection = sqlite3.connect('test.db')
        self.cur = connection.cursor()

    def create_table(self):
        self.cur.execute("CREATE TABLE IF NOT EXISTS buyorders (ticker TEXT, type TEXT, strike INTEGER, price FLOAT, ordertime TEXT)")

    def delete_table(self):
        self.cur.execute("DROP TABLE buyorders")

    def save_data(self, ticker: str, type: str, strike: int, price:float, ordertime: str):
        self.cur.execute(f"INSERT INTO buyorders VALUES ({ticker}, {type}, {strike}, {price}, {ordertime})")

    # def save_data(self, order: Order):
    #     self.cur.execute(f"INSERT INTO buyorders VALUES ({order.ticker}, {order.type}, {order.strike}, {order.price}, {order.ordertime})")
