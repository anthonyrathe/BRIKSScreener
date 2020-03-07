from src.data_processing.RawDataProcessor import RawDataProcessor
from src.data_processing.DataReader import DataReader
from src.data_processing.TickerLoader import TickerLoader
import pandas as pd
import os
import datetime
from os.path import dirname as dirname

tl = TickerLoader()
tickers = tl.getTickers(name='sec',filterTickers=True)

# Parameters
SECDelay = 0

count = 0
amount = len(tickers)

snapshot = pd.DataFrame()

for ticker in tickers:
	try:
		count += 1
		print("{}: {}%".format(ticker,count/amount*100))

		reader = DataReader(ticker)
		processor = RawDataProcessor(reader,update=['prices'])

		result = processor.process(version='overview',SECDelay=SECDelay, ticker=ticker).iloc[-1,:]
		result.name = ticker
		snapshot = snapshot.append(result)
		processor.storeResult()

	except Exception as e:
		continue

date = datetime.date.today()
path = os.path.relpath("{}/data/cleaned/snapshot/sec_{}.csv".format(dirname(dirname(dirname(__file__))),str(date)))
snapshot.to_csv(path,index=True)