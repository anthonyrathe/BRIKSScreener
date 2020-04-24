from src.data_processing.DataReader import DataReader
from src.data_processing.RawDataProcessor import RawDataProcessor
import numpy as np


all_groups = [['C', 'JPM', 'TCF', 'UMBF'], ['PFE', 'JNJ', 'ABT', 'BMY'], ['INTC', 'XLNX', 'KOPN', 'MXIM'],
			  ['MCD', 'EAT', 'JACK', 'PZZA'], ['LH', 'AMS', 'DGX', 'PMD'], ['COHR', 'PKI', 'BIO', 'WAT'],
			  ['MMM', 'TFX', 'CRY', 'ATRI'], ['TRT', 'IVAC', 'ASYS', 'VECO'], ['GGG', 'FLS', 'ITT', 'IEX'],
			  ['AVX', 'HUBB', 'IIN', 'MRCY'], ['FLEX', 'CTS', 'IEC', 'SANM'], ['HDSN', 'KAMN', 'LAWS', 'WLFC'],
			  ['CIA', 'AAME', 'FFG', 'GL'], ['CIGI', 'FRPH', 'CTO', 'TRC'], ['NBIX', 'BCRX', 'TECH', 'TTNP'],
			  ['SCON', 'MSI', 'BKTI', 'VSAT'], ['LECO', 'CVR', 'SPXC', 'PFIN'], ['STRM', 'EBIX', 'UIS', 'JKHY'], #['MARPS', 'SJT', 'TPL', 'PBT'],
			  ['UVV', 'STKL', 'ANDE', 'PYX'], ['BZH', 'NVR', 'PHM', 'MTH'], ['MOD', 'DORM', 'STRT', 'SUP'],
			  ['PCAR', 'SPAR', 'F', 'OSK'], ['HLX', 'CLB', 'ENSV', 'RES'], ['BCPC', 'FMC', 'GRA', 'OLN']]
tickers = list(np.array(all_groups).flatten())

# Problematic tickers:
# - MTSL (revenueGrowth1y)
# - PUK (revenueGrowth1y)
# - LFC (revenueGrowth1y)
# - SAN (revenueGrowth1y)
# - ING (revenueGrowth1y)
# - BBAR (revenueGrowth1y)
problematic = ['MTSL','PUK','LFC','SAN','ING','BBAR']


# for ticker in ['C',]:
# 	print(ticker)
# 	reader = DataReader(ticker)
# 	reader.loadRaw(update=['fundamentals'],forceRefetch=False)
# 	processor = RawDataProcessor(reader)
# 	processor.processV2(1,1,fix_received_dates=True,fix_missing_data=True)
# 	processor.storeResult()

sectors = []
for group in all_groups:
	print(group)
	ticker = group[0]
	reader = DataReader(ticker)
	reader.loadRaw(update=[],forceRefetch=False)
	fundamentals = reader.getFundamentals()
	a_filing = fundamentals.iloc[0]
	peer_group = a_filing['sicdescription']
	sectors.append(peer_group)
	print(peer_group)
print(sectors)