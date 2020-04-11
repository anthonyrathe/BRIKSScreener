from src.data_processing.DataReader import DataReader
from src.data_processing.RawDataProcessor import RawDataProcessor
import numpy as np

groups = [['PEP','KO','KDP','MNST'],['BBY','TGT','WMT','COST'],['FB','GOOGL','AAPL','AMZN']]
tickers = list(np.array(groups).flatten())

for ticker in tickers:
	print(ticker)
	reader = DataReader(ticker)
	#reader.loadRaw(update=['fundamentals','prices','general'],forceRefetch=True)
	processor = RawDataProcessor(reader)
	processor.processV2(1,1,fix_received_dates=True,fix_missing_data=True)
	processor.storeResult()