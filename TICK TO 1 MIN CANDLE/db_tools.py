import psycopg2
from psycopg2 import sql

class DataUploader:
    def __init__(self, dbname, user, host, password):
        """Initialize database connection."""
        self.conn = None
        try:
            self.conn = psycopg2.connect(f"dbname='{dbname}' user='{user}' host='{host}' password='{password}'")
        except Exception as e:
            print("Failed to connect to the database:", str(e))

    def insert_data(self, data):
        """Insert data into the database."""
        if self.conn:
            with self.conn.cursor() as cursor:
                query = sql.SQL("""
                    INSERT INTO mindata (datetime, open, high, low, close, volume, oi, ticker)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """)
                try:
                    cursor.execute(query, (
                        data['timestamp'],
                        data['open'],
                        data['high'],
                        data['low'],
                        data['close'],
                        data['volume'],
                        data['oi'],
                        data['symbol']
                    ))
                    self.conn.commit()  # Commit to save changes
                except Exception as e:
                    print("Failed to insert data:", str(e))
                    self.conn.rollback()  # Rollback in case of error

    def close_connection(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()


# example
if __name__ == "__main__":
    uploader = DataUploader('optiondata', 'postgres', 'localhost', '12345')
    ohlc = {
        'symbol': 123456,  # Example ticker
        'open': 1500,
        'high': 1550,
        'low': 1490,
        'close': 1525,
        'volume': 10000,
        'oi': 500,
        'timestamp': '2024-06-06 12:00:00'
    }
    uploader.insert_data(ohlc)
    uploader.close_connection()
