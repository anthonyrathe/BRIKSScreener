import sys
sys.path.append("/home/anthonyrathe/repos/BRIKSScreener")
from src.overview_generation.OverviewGenerator import OverviewGenerator
from src.data_processing.TickerLoader import TickerLoader
from src.exceptions.NoDataFoundException import NoDataFoundException
import os
from os.path import dirname as dirname

tl = TickerLoader()
exchanges = ['sec','frankfurt','euronext','lse']

def strip_exchange_code(str):
	i = str.find('.')
	if i > -1:
		return str[:i]
	else:
		return str

def generate_overviews(exchange):
	if exchange == 'sec':
		tickers = tl.getTickers(name='sec',filterTickers=True)
	else:
		tickers = tl.getTickers(exchange,filterTickers=False,exchangeSuffix=True)

	tickers = sorted(tickers)
	generator = OverviewGenerator()
	start = tickers[0]
	for i in range(len(tickers)):
		ticker = tickers[i]
		print("{}: {} - {}%".format(exchange,ticker,round(i/len(tickers)*100,2)),end=" ")
		try:
			if not generator.generateOverview(ticker,update=[]):
				generator.save("{}_{}-{}".format(exchange,strip_exchange_code(start),strip_exchange_code(ticker)))
				generator = OverviewGenerator()
				if i < len(tickers) - 1:
					start = tickers[i+1]
				else:
					break
			print("ok")
		except NoDataFoundException as e:
			print(e.message)
			continue
		except KeyError:
			print("")
			continue

	generator.save("{}_{}-{}".format(exchange,strip_exchange_code(start),strip_exchange_code(tickers[-1])))

base_path = "{}/data/cleaned/overview_ppt/".format(dirname(dirname(dirname(__file__))))
files = os.listdir(os.path.relpath(base_path))
for file in files:
	os.remove("{}/{}".format(base_path,file))

for exchange in exchanges[1:]:
	generate_overviews(exchange)



