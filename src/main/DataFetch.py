from src.data_processing.DataReader import DataReader
from src.data_processing.TickerLoader import TickerLoader

tl = TickerLoader()
tickers = tl.getTickers(name='sec',filterTickers=True)

count = 0
amount = len(tickers)

for ticker in tickers:
	try:
		count += 1
		print("{}: {}%".format(ticker,count/amount*100))

		reader = DataReader(ticker)
		reader.loadRaw(update=['fundamentals','prices','general'],forceRefetch=True)

	except Exception as e:
		continue