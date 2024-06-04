import os
from multiprocessing import Process, Queue
from dotenv import load_dotenv
from pydantic import BaseModel
from websocketInstrument import OptionDataStreaming
from getdata import getDataTools
load_dotenv()

class Setting(BaseModel):
    API_KEY: str = os.getenv('API_KEY')
    API_SECRET: str = os.getenv('API_SECRET')
    REQUEST_TOKEN: str = os.getenv('REQUEST_TOKEN')
    ACCESS_TOKEN: str = os.getenv('ACCESS_TOKEN')


if __name__ == "__main__":
    settings = Setting()
    q = Queue()
    getData = getDataTools(settings)
    Process(target=getData.get_data, args=(q, )).start()
    OptionDataStreaming_process = OptionDataStreaming(settings, q)
