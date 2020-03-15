import urllib.request
import numpy as np

import requests
import os
import yaml
import re
import pandas as pd
import datetime as dt
import io
from os.path import dirname as dirname
import json, datetime
from forex_python.converter import CurrencyRates

def scrapeAll(ticker):
	other_details_json_link = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{0}?formatted=true&lang=en-US&region=US&modules=pageViews%2CesgScores%2CsummaryDetail%2CinstitutionOwnership%2CmajorHoldersBreakdown%2CnetSharePurchaseActivity%2CinsiderHolders%2CinsiderTransactions%2CfundOwnership%2Cprice%2Cdetails%2CmajorDirectHolders%2CsummaryProfile%2CquoteType%2CassetProfile%2CfinancialData%2CrecommendationTrend%2CupgradeDowngradeHistory%2Cearnings%2CdefaultKeyStatistics%2CcalendarEvents%2CincomeStatementHistory%2CcashflowStatementHistory%2CbalanceSheetHistory&corsDomain=finance.yahoo.com".format(ticker)
	summary_json_response = requests.get(other_details_json_link)
	json_loaded_summary =  json.loads(summary_json_response.text)
	#print(json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['sharesOutstanding']['raw'])
	return json_loaded_summary

# For more info on Yahoo Finance data: https://rapidapi.com/apidojo/api/yahoo-finance1?endpoint=apiendpoint_2e0b16d4-a66b-469e-bc18-b60cec60661b

class YahooFinanceScraper:

	def __init__(self, ticker):
		self.ticker = ticker
		self.prices = None
		self.general = None
		self.fundamental = None

	def scrapeAllPrices(self):

		dateTimeFormat = "%Y%m%d %H:%M:%S"

		def parseStr(s):
			""" convert string to a float or string """
			f = s.strip()
			if f[0] == '"':
				return f.strip('"')
			elif f=='N/A':
				return np.nan

			else:
				try: # try float conversion
					prefixes = {'M':1e6, 'B': 1e9}
					prefix = f[-1]

					if prefix in prefixes: # do we have a Billion/Million character?
						return float(f[:-1])*prefixes[prefix]
					else:                       # no, convert to float directly
						return float(f)
				except ValueError: # failed, return original string
					return s



		def getQuote(symbols):
			"""
			get current yahoo quote

			Parameters
			-----------
			symbols : list of str
				list of ticker symbols

			Returns
			-----------
			DataFrame , data is row-wise
			"""

			# for codes see: http://www.gummy-stuff.org/Yahoo-data.htm
			if not isinstance(symbols,list):
				symbols = [symbols]


			header =               ['symbol','last','change_pct','PE','time','short_ratio','prev_close','eps','market_cap']
			request = str.join('', ['s',     'l1',     'p2'  ,   'r', 't1',     's7',        'p',       'e'     , 'j1'])


			data = dict(list(zip(header,[[] for i in range(len(header))])))

			urlStr = 'http://finance.yahoo.com/d/quotes.csv?s=%s&f=%s' % (str.join('+',symbols), request)

			try:
				lines = urllib.request.urlopen(urlStr).readlines()
			except Exception as e:
				s = "Failed to download:\n{0}".format(e);
				print(s)

			for line in lines:
				fields = line.decode().strip().split(',')
				#print fields, len(fields)
				for i,field in enumerate(fields):
					data[header[i]].append( parseStr(field))

			idx = data.pop('symbol')


			return pd.DataFrame(data,index=idx)


		def getHistoricData(symbols, **options):
			"""
			get data from Yahoo finance and return pandas dataframe
			Will get OHLCV data frame if sinle symbol is provided.
			If many symbols are provided, it will return a wide panel

			Parameters
			------------
			symbols : str or list
				Yahoo finanance symbol or a list of symbols
			sDate : tuple  (optional)
				start date (y,m,d)
			adjust : bool
				T/[F] adjust data based on adj_close

			Returns
			---------
			Panel

			"""

			assert isinstance(symbols,(list,str)), 'Input must be a string symbol or a list of symbols'

			if isinstance(symbols,str):
				return getSymbolData(symbols,**options)
			else:
				data = {}
				print('Downloading data:')
				for idx,symbol in enumerate(symbols):
					data[symbol] = getSymbolData(symbol,verbose=False,**options)

				return pd.Panel(data)

		def getSymbolData(symbol, sDate=(1950,1,1), adjust=False, verbose=True):
			"""
			get data from Yahoo finance and return pandas dataframe
			Parameters
			-----------
			symbol : str
				Yahoo finanance symbol
			sDate : tuple , optional
				start date (y,m,d), defaults to 1 jan 1990
			adjust : bool , optional
				use adjusted close values to correct OHLC. adj_close will be ommited
			verbose : bool , optional
				print output

			Returns
			---------
				DataFrame

			"""

			period1 = int(dt.datetime(*sDate).timestamp()) # convert to seconds since epoch
			period2 = int(dt.datetime.now().timestamp())

			params = (symbol, period1, period2, _token['crumb'])

			url = "https://query1.finance.yahoo.com/v7/finance/download/{0}?period1={1}&period2={2}&interval=1d&events=history&crumb={3}".format(*params)

			data = requests.get(url, cookies={'B':_token['cookie']})

			buf = io.StringIO(data.text) # create a buffer
			df = pd.read_csv(buf,index_col=0,parse_dates=True) # convert to pandas DataFrame

			# rename columns
			newNames = [c.lower().replace(' ','_') for c in df.columns]
			renames = dict(zip(df.columns,newNames))
			df = df.rename(columns=renames)

			if verbose:
				print(('Got %i days of data' % len(df)))

			if adjust:
				return _adjust(df,removeOrig=True)
			else:
				return df

		def _adjust(df, removeOrig=False):
			"""
		  _adjustust hist data based on adj_close field
			"""
			c = df['close']/df['adj_close']

			df['adj_open'] = df['open']/c
			df['adj_high'] = df['high']/c
			df['adj_low'] = df['low']/c

			if removeOrig:
				df=df.drop(['open','close','high','low'],axis=1)
				renames = dict(list(zip(['adj_open','adj_close','adj_high','adj_low'],['open','close','high','low'])))
				df=df.rename(columns=renames)

			return df


		def loadToken():
			"""
			get cookie and crumb from APPL page or disk.
			force = overwrite disk data
			"""
			refreshDays = 30 # refreh cookie every x days

			# set destinatioin file
			dataDir = os.path.expanduser('~')+'/twpData'
			dataFile = dataFile = os.path.join(dataDir,'yahoo_cookie.yml')

			try : # load file from disk

				data = yaml.load(open(dataFile,'r'))
				age = (dt.datetime.now()- dt.datetime.strptime(  data['timestamp'], dateTimeFormat) ).days
				assert age < refreshDays, 'cookie too old'

			except (AssertionError,FileNotFoundError):     # file not found

				if not os.path.exists(dataDir):
					os.mkdir(dataDir)

				data = getToken(dataFile)

			return data


		def getToken(fName = None):
			""" get cookie and crumb from yahoo """

			url = 'https://finance.yahoo.com/quote/AAPL/history' # url for a ticker symbol, with a download link
			r = requests.get(url)  # download page

			txt = r.text # extract html

			cookie = r.cookies['B'] # the cooke we're looking for is named 'B'

			pattern = re.compile('.*"CrumbStore":\{"crumb":"(?P<crumb>[^"]+)"\}')

			for line in txt.splitlines():
				m = pattern.match(line)
				if m is not None:
					crumb = m.groupdict()['crumb']

			assert r.status_code == 200 # check for succesful download

			# save to disk
			data = {'crumb': crumb, 'cookie':cookie, 'timestamp':dt.datetime.now().strftime(dateTimeFormat)}

			if fName  is not None: # save to file
				with open(fName,'w') as fid:
					yaml.dump(data,fid)

			return data

		#-------------- get token
		_token = loadToken() # get token from disk or yahoo

		self.prices = getSymbolData(self.ticker,verbose=False)

	def storeAllPrices(self):
		if self.prices is None:
			self.scrapeAllPrices()

		path = os.path.relpath("{}/data/raw/prices/{}.csv".format(dirname(dirname(dirname(__file__))),self.ticker))
		self.prices.to_csv(path,index=True)

	def scrapeGeneral(self):

		self.general = scrapeAll(self.ticker)

	def storeGeneral(self):
		if self.general is None:
			self.scrapeGeneral()

		path = os.path.relpath("{}/data/raw/general/{}.csv".format(dirname(dirname(dirname(__file__))),self.ticker))
		with open(path, mode='w') as file:
			file.write(str(self.general))

	def scrapeFundamentals(self):
		self.scrapeGeneral()

		balancesheets = self.general['quoteSummary']['result'][0]['balanceSheetHistory']['balanceSheetStatements']
		cashflowstatements = self.general['quoteSummary']['result'][0]['cashflowStatementHistory']['cashflowStatements']
		incomestatements = self.general['quoteSummary']['result'][0]['incomeStatementHistory']['incomeStatementHistory']

		statements = (balancesheets, cashflowstatements, incomestatements)
		for statement in statements:
			for line in statement:
				del line['maxAge']

		def finalValue(key, value):
			if key == 'endDate':
				return datetime.datetime.strptime(value['fmt'],'%Y-%m-%d')
			else:
				try:
					return value['raw']
				except KeyError:
					return None

		for statement in statements:
			for index in range(len(statement)):
				statement[index] = {key:finalValue(key,value) for key, value in statement[index].items()}

		self.fundamental = {'balanceSheets':balancesheets, 'cashflowStatements': cashflowstatements, 'incomeStatements':incomestatements}

	def getSnapshot(self):

		try:
			self.scrapeFundamentals()

			result = pd.DataFrame()
			for statement in self.fundamental.values():
				new = pd.DataFrame(statement)
				new = new.set_index(new.endDate)
				shares_outstanding = self.general['quoteSummary']['result'][0]['defaultKeyStatistics']['sharesOutstanding']['raw']
				current_price = self.general['quoteSummary']['result'][0]['financialData']['currentPrice']['raw']
				#new['market_cap'] = shares_outstanding*current_price
				new['market_cap'] = self.general['quoteSummary']['result'][0]['summaryDetail']['marketCap']['raw']
				result = pd.concat((result,new),axis=1)

			result = result.loc[:, ~result.columns.duplicated()]

			result['currencycode_exchange'] = self.general['quoteSummary']['result'][0]['financialData']['financialCurrency']
			result['currencycode_earnings'] = self.general['quoteSummary']['result'][0]['earnings']['financialCurrency']
			result['conversion_rate_earnings_to_exchange'] = CurrencyRates().convert(self.general['quoteSummary']['result'][0]['earnings']['financialCurrency'],self.general['quoteSummary']['result'][0]['financialData']['financialCurrency'],1)

			result['EBITDA'] = result['depreciation']+result['ebit']
			result['revenueGrowth1y'] = result['totalRevenue']/result['totalRevenue'].shift(-1)-1
			result['EBITDAGrowth1y'] = result['EBITDA']/result['EBITDA'].shift(-1)-1
			result['earningsGrowth1y'] = result['netIncome']/result['netIncome'].shift(-1)-1

			result = result.sort_index(ascending=False).iloc[0,:]
			result['name'] = self.general['quoteSummary']['result'][0]['quoteType']['longName']
			for i in range(len(self.general['quoteSummary']['result'][0]['recommendationTrend']['trend'])):
				result['strongBuy'] = self.general['quoteSummary']['result'][0]['recommendationTrend']['trend'][i]['strongBuy']
				result['buy'] = self.general['quoteSummary']['result'][0]['recommendationTrend']['trend'][i]['buy']
				result['hold'] = self.general['quoteSummary']['result'][0]['recommendationTrend']['trend'][i]['hold']
				result['sell'] = self.general['quoteSummary']['result'][0]['recommendationTrend']['trend'][i]['sell']
				result['strongSell'] = self.general['quoteSummary']['result'][0]['recommendationTrend']['trend'][i]['strongSell']
				if (result['strongBuy'] + result['buy'] + result['hold'] + result['sell'] + result['strongSell']) > 0: break

			result['industry'] = self.general['quoteSummary']['result'][0]['assetProfile']['industry']
			result['sector'] = self.general['quoteSummary']['result'][0]['assetProfile']['sector']

			result['price'] = self.general['quoteSummary']['result'][0]['financialData']['currentPrice']['raw']

			if result['currencycode_exchange'] == 'GBP':
				# This is actually GBp
				#result['market_cap'] /= 100
				result['price'] /= 100


			return result
		except Exception:
			return None

#scraper = YahooFinanceScraper('AFX.L')
#print(scraper.getSnapshot())
