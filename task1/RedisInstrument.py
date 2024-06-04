"""
Redis db data saver and caller
"""
import redis
import json


class InstrumentDumpFetch:

    def __init__(self):
        self.conn = redis.StrictRedis(host='localhost', port=6379)

    def data_dump(self, token, instrument_data):
        try:
            return self.conn.set(token, json.dumps(instrument_data))
        except Exception as e:
            print(e)

    def fetch_token(self, token):
        try:
            token_instrument = json.loads(self.conn.get(token))
        except Exception as e:
            raise Exception('Error {}'.format(e))
        return token_instrument

    