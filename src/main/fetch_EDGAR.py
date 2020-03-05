from src.data_processing.DataReader import DataReader
from src.data_processing.TickerLoader import TickerLoader
from src.exceptions.APILimitExceededException import APILimitExceededException
from src.exceptions.NoDataFoundException import NoDataFoundException
import ast

tl = TickerLoader()
tl.scrapeTickers('sec')
tickers = tl.getTickers(name='sec',filterTickers=True)

with open('./progress','r') as f:
	counts = ast.literal_eval(f.readline())
	print(counts)
	count = counts['sec']
amount = len(tickers)


while True:
	count = count % amount
	ticker = tickers[count]
	try:
		count += 1
		print("{}: {}%".format(ticker,count/amount*100))

		reader = DataReader(ticker)
		reader.loadRaw(update=['fundamentals','general'],forceRefetch=True)


	except NoDataFoundException:
		print("No data")
	except APILimitExceededException:
		break

	counts['sec'] = count
	with open('progress','w+') as f:
		f.truncate(0)
		f.write(str(counts))
