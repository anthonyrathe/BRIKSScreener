import sys
sys.path.append("/home/anthonyrathe/repos/BRIKSScreener")
from os.path import dirname as dirname
import os
from src.data_processing.DataReader import DataReader
from src.exceptions.NoDataFoundException import NoDataFoundException
import pandas as pd

groups = {}

fundamentals_path = os.path.relpath("{}/data/raw/fundamentals/".format(dirname(dirname(dirname(__file__)))))
prices_path = os.path.relpath("{}/data/raw/prices/".format(dirname(dirname(dirname(__file__)))))
general_path = os.path.relpath("{}/data/raw/general/".format(dirname(dirname(dirname(__file__)))))
fundamentals_tickers = [s[:-4] for s in os.listdir(fundamentals_path)]
prices_tickers = [s[:-4] for s in os.listdir(prices_path)]
general_tickers = [s[:-4] for s in os.listdir(general_path)]
tickers = [t for t in fundamentals_tickers if (t in prices_tickers) and (t in general_tickers)]

ticker_data = []
for ticker in tickers:
	try:
		reader = DataReader(ticker)
		reader.loadRaw()
	except NoDataFoundException:
		continue

	fundamentals = reader.getFundamentals()
	fundamentals['periodenddate'] = pd.to_datetime(fundamentals['periodenddate'],format="%m/%d/%Y")
	first_filing_date = fundamentals['periodenddate'].min()
	first_filing = fundamentals.loc[fundamentals.periodenddate==first_filing_date]
	peer_group = first_filing['sicdescription'].values[0]
	conversion_rate = first_filing['usdconversionrate'].values[0]

	if (conversion_rate == 1.0):
		ticker_data.append((first_filing_date,peer_group,ticker))

ticker_data = sorted(ticker_data,key=lambda x: x[0])

peer_group_count = 25
peer_group_size = 4
def finished(groups):
	group_sizes = [int(len(v)>=peer_group_size) for _,v in groups.items()]
	return sum(group_sizes) >= peer_group_count

peer_groups = dict()
for date,peer_group,ticker in ticker_data:
	#if finished(peer_groups):
	#	break

	if peer_group not in peer_groups.keys():
		peer_groups[peer_group] = []

	peer_groups[peer_group].append((ticker,date))

peer_groups = [group[:peer_group_size] for group in peer_groups.values() if len(group)>=peer_group_size]
peer_groups = sorted(peer_groups,key=lambda x: x[-1][1])
peer_groups = [[t for t, _ in peer_group[:peer_group_size]] for peer_group in peer_groups[:peer_group_count]]
print(peer_groups)