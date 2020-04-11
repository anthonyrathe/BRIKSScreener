from src.data_processing.DataReader import DataReader
from src.data_processing.RawDataProcessor import RawDataProcessor
import numpy as np

#groups = [['PEP','KO','KDP','MNST'],['BBY','TGT','WMT','COST'],['FB','GOOGL','AAPL','AMZN']]
groups = [['BYFC', 'PROV', 'CASH', 'ETFC'], ['MSFT', 'GEC', 'PTC', 'AWRE'], ['AKRX', 'SNGX', 'ABEO', 'ARWR'], ['FLEX', 'IEC', 'SANM', 'PLXS'], ['APYX', 'CRY', 'ATRI', 'ABMD'], ['IOR', 'ALX', 'NNN', 'DRE'], ['GHC', 'PRDO', 'STRA', 'ATGE'], ['PDCE', 'CPE', 'OXY', 'TTI'], ['WRB', 'UVE', 'MCY', 'HIG'], ['PEG', 'PCG', 'ED', 'AVA'], ['UMBF', 'BANF', 'CFBK', 'SNV'], ['FCCO', 'CNOB', 'FBSS', 'CZNC'], ['AMD', 'RMBS', 'POWI', 'AMRH'], ['BCRX', 'TECH', 'TTNP', 'PLX'], ['RJF', 'GBL', 'SIEB', 'WDR'], ['GEOS', 'ROK', 'TRMB', 'TMO'], ['MARPS', 'SJT', 'TPL', 'NRT'], ['COHU', 'ITRI', 'TRNS', 'AEHR'], ['STMP', 'EBAY', 'CASS', 'TNET'], ['STRT', 'SUP', 'THRM', 'MPAA'], ['ASGN', 'VOLT', 'RCMT', 'AMN'], ['VZ', 'ATNI', 'CBB', 'TKC'], ['WSTL', 'JCS', 'DZSI', 'MTSL'], ['CIA', 'AAME', 'PUK', 'LFC'], ['SAN', 'RY', 'ING', 'BBAR']]
tickers = list(np.array(groups).flatten())

# Problematic tickers:
# - MTSL (revenueGrowth1y)
# - PUK (revenueGrowth1y)
# - LFC (revenueGrowth1y)
# - SAN (revenueGrowth1y)
# - ING (revenueGrowth1y)
# - BBAR (revenueGrowth1y)


for ticker in tickers[tickers.index('ING')+1:]:
	print(ticker)
	reader = DataReader(ticker)
	reader.loadRaw(update=['fundamentals','prices','general'],forceRefetch=True)
	processor = RawDataProcessor(reader)
	processor.processV2(1,1,fix_received_dates=True,fix_missing_data=True)
	processor.storeResult()