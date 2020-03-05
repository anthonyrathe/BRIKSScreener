import datetime, json, os
import requests
import pandas as pd
from os.path import dirname as dirname
from src.exceptions.NoDataFoundException import NoDataFoundException
from src.exceptions.APILimitExceededException import APILimitExceededException

class SECScraper:
	"""
	This scraper fetches all fundamental data for a given company.
	The data is then stored in a database.
	Records can be added or overwritten.
	"""

	def __init__(self, ticker):
		self.ticker = ticker
		self.result = None

	def scrapeAllQuarterFilings(self):
		# Global variables
		EDGARCoreFinancialappkey="usjam7jt5zcvmq8s3s22sadq"

		# Fetches the core financial data of a company during four quarters in JSON format.
		def get_core_financials_8qtrs(symbol, end_year, end_quarter, appkey=EDGARCoreFinancialappkey):
			parameters = {"primarysymbols": str(symbol), "fiscalperiod": "1980q1~"+str(end_year)+"q"+str(end_quarter), "appkey": str(appkey)}

			# Make a get request with the parameters.
			response = requests.get("http://datafied.api.edgar-online.com/v2/corefinancials/qtr?primarysymbols="+parameters['primarysymbols']
									+ "&fiscalperiod=" + str(parameters['fiscalperiod']) + "&appkey=" + str(parameters['appkey']))

			return json.loads(response.content.decode())

		# Fetches the core financial data of a company during and between specified quarters.
		# Preferably pick an even amount of quarters, since an even amount of results will always be provided
		def get_core_financials(symbol, start_year, start_quarter, end_year, end_quarter):
			last_request_time = datetime.datetime.now()
			year = end_year
			qrtr = end_quarter
			stop = False
			data = []
			while year >= start_year and qrtr >= start_quarter and not stop:
				while True:
					if datetime.datetime.now() - last_request_time >= datetime.timedelta(milliseconds=500):
						new_data = get_core_financials_8qtrs(symbol, year, qrtr)
						last_request_time = datetime.datetime.now()
						break
				year -= 2
				if "Message" in new_data.keys():
					if "rate limit" in new_data['Message']:
						raise APILimitExceededException
					elif "Result set is empty due to an entitlement." in new_data['Message']:
						raise NoDataFoundException
					elif "Parameter 'primarysymbols' has invalid value. Only a comma can be used to separate tickers. For dual class stocks, only a period or apostrophe are recognized characters." in new_data['Message']:
						raise NoDataFoundException
					else:
						raise Exception(new_data['Message'])
				else:
					for i in range(8):
						if len(new_data['result']['rows']) > i:
							data.append(new_data['result']['rows'][i]['values'])
							#print(new_data['result']['rows'][i]['values'])
						else:
							stop = True
							return data
			return data

		# Fetches all core finanical data of a company from the first quarter of 2000 until the last of the current year (if
		# available)
		def get_all_core_financials(symbol):
			return get_core_financials(symbol, 2000, 1, datetime.datetime.now().year+5, 4)

		unformatted = get_all_core_financials(self.ticker)
		result = pd.DataFrame()
		for entry in unformatted:
			line = pd.Series({dataPoint['field']:dataPoint['value'] for dataPoint in entry})
			result = result.append(line, ignore_index=True)

		if len(result.index) > 1:
			self.result = result.drop_duplicates()
		else:
			raise NoDataFoundException

	def storeAllQuarterFilings(self):
		if self.result is None:
			self.scrapeAllQuarterFilings()

		path = os.path.relpath("{}/data/raw/fundamentals/{}.csv".format(dirname(dirname(dirname(__file__))),self.ticker))

		if os.path.isfile(path):
			old_data = pd.read_csv(path,index_col='receiveddate', parse_dates=True).sort_values(by='receiveddate')
			new_data = self.result
			pd.concat((old_data,new_data),ignore_index=True,sort=True).sort_values(by='receiveddate').drop_duplicates(keep='last')

		self.result.to_csv(path,index=False)

# NOTES
# - The SEC will override changed reports, which leads to reports being submitted much too late. This does not impair the
#	validity of the simulation, however it does mean that the simulation will think that some reports are missing, while
# 	in reality they will already have been filed. The simulation will extrapolate the received data, while in reality we
#	will be working with all data. This could lead to discrepancies between reality and simulation. However, we will assume
# 	that preliminary numbers will not differ significantly from extrapolation of previous numbers.
#		UPDATE: when using the latest year data as test set (this is probably the best appx. of reality), we obtained
#			even better results
