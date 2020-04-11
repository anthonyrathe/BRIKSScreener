from src.data_processing.DataReader import DataReader
from src.data_processing.RawDataProcessor import RawDataProcessor
import numpy as np

#groups = [['PEP','KO','KDP','MNST'],['BBY','TGT','WMT','COST'],['FB','GOOGL','AAPL','AMZN']]
old_groups = [['BYFC', 'PROV', 'CASH', 'ETFC'], ['MSFT', 'GEC', 'PTC', 'AWRE'], ['AKRX', 'SNGX', 'ABEO', 'ARWR'], ['FLEX', 'IEC', 'SANM', 'PLXS'], ['APYX', 'CRY', 'ATRI', 'ABMD'], ['IOR', 'ALX', 'NNN', 'DRE'], ['GHC', 'PRDO', 'STRA', 'ATGE'], ['PDCE', 'CPE', 'OXY', 'TTI'], ['WRB', 'UVE', 'MCY', 'HIG'], ['PEG', 'PCG', 'ED', 'AVA'], ['UMBF', 'BANF', 'CFBK', 'SNV'], ['FCCO', 'CNOB', 'FBSS', 'CZNC'], ['AMD', 'RMBS', 'POWI', 'AMRH'], ['BCRX', 'TECH', 'TTNP', 'PLX'], ['RJF', 'GBL', 'SIEB', 'WDR'], ['GEOS', 'ROK', 'TRMB', 'TMO'], ['MARPS', 'SJT', 'TPL', 'NRT'], ['COHU', 'ITRI', 'TRNS', 'AEHR'], ['STMP', 'EBAY', 'CASS', 'TNET'], ['STRT', 'SUP', 'THRM', 'MPAA'], ['ASGN', 'VOLT', 'RCMT', 'AMN'], ['VZ', 'ATNI', 'CBB', 'TKC'], ['WSTL', 'JCS', 'DZSI', 'MTSL'], ['CIA', 'AAME', 'PUK', 'LFC'], ['SAN', 'RY', 'ING', 'BBAR']]
groups = [['C', 'JPM', 'TCF', 'UMBF'], ['PFE', 'JNJ', 'CAPR', 'AKRX'], ['VZ', 'T', 'FTR', 'SHEN'], ['INTC', 'XLNX', 'EMAN', 'KOPN'], ['LEA', 'MOD', 'DORM', 'STRT'], ['MCD', 'EAT', 'JACK', 'PZZA'], ['TGNA', 'NTN', 'CETV', 'SSP'], ['ZIXI', 'TCX', 'CSGS', 'RAMP'], ['KSU', 'CSX', 'UNP', 'NSC'], ['ARTNA', 'AWR', 'MSEX', 'YORW'], ['PCAR', 'SPAR', 'F', 'OSK'], ['TESS', 'ARW', 'TAIT', 'UUU'], ['GLBZ', 'TRST', 'GSBC', 'FCCO'], ['UAL', 'SKYW', 'ALK', 'AAL'], ['DAIO', 'TER', 'COHU', 'ITRI'], ['LNG', 'NJR', 'SJI', 'ATO'], ['NTIP', 'VHC', 'ACTG', 'REFR'], ['MUX', 'AUMN', 'VGZ', 'GSS'], ['FRHC', 'RJF', 'GBL', 'SIEB'], ['NBIX', 'BCRX', 'TECH', 'TTNP'], ['GHC', 'PRDO', 'STRA', 'ATGE'], ['FCAP', 'BYFC', 'PROV', 'CASH'], ['MARPS', 'SJT', 'TPL', 'PBT'], ['RIG', 'DO', 'VAL', 'PTEN'], ['WTS', 'CIR', 'PH', 'HLIO']]
old_tickers = list(np.array(old_groups).flatten())
tickers = list(np.array(groups).flatten())

# Problematic tickers:
# - MTSL (revenueGrowth1y)
# - PUK (revenueGrowth1y)
# - LFC (revenueGrowth1y)
# - SAN (revenueGrowth1y)
# - ING (revenueGrowth1y)
# - BBAR (revenueGrowth1y)
problematic = ['MTSL','PUK','LFC','SAN','ING','BBAR']


for ticker in tickers:
	if ticker in old_tickers:
		continue
	print(ticker)
	reader = DataReader(ticker)
	#reader.loadRaw(update=['fundamentals','prices','general'],forceRefetch=True)
	processor = RawDataProcessor(reader)
	processor.processV2(1,1,fix_received_dates=True,fix_missing_data=True)
	processor.storeResult()