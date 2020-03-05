from src.data_processing.DataReader import DataReader
from src.data_processing.RawDataProcessor import RawDataProcessor
import pandas as pd

tickers = pd.read_csv('s&p500_jan2014.csv',delimiter=';')
tickers = list(tickers['Ticker symbol'])
tickers = tickers[tickers.index('POM'):]
print(tickers)

for i in range(len(tickers)):
	ticker = tickers[i]
	print("{}: {}%".format(ticker,i/len(tickers)*100))
	try:
		reader = DataReader(ticker)
		processor = RawDataProcessor(reader)
		processor.processV2(1,1)
		processor.storeResult()
		#print(processor.addTimeSeriesFeatures().head())
	except BaseException as e:
		print("Something went wrong when obtaining the raw data...")

# from src.data_processing.TickerLoader import TickerLoader
# loader = TickerLoader()
# loader.scrapeTickers('euronext')
