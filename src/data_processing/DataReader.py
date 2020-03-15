import pandas as pd
import numpy as np
import os, math, ast
from src.data_collecting.SECScraper import SECScraper
from src.data_collecting.YahooFinanceScraper import YahooFinanceScraper
from src.data_processing.RawDataProcessor import RawDataProcessor
from src.exceptions.NoDataFoundException import NoDataFoundException
from os.path import dirname as dirname
import datetime

class DataReader:

	def __init__(self, ticker):
		self.ticker = ticker

		self.fundamentals = None
		self.prices = None
		self.sharesOutstanding = None
		self.marketCap = None
		self.general = None
		self.cleaned = None

		self.lastLatestFundamentalsResult = [None, {'incomestatement':None, 'balancesheet':None}] # Last received date, result

		self.incomeStatementFields = ['capitalexpenditures','cfdepreciationamortization','changeinaccountsreceivable','changeincurrentassets','changeincurrentliabilities','changeininventories','costofrevenue','dividendspaid','ebit','grossprofit','incomebeforetaxes','investmentchangesnet','netchangeincash','netincome','netincomeapplicabletocommon','researchdevelopmentexpense','retainedearnings','sellinggeneraladministrativeexpenses','totalrevenue']
		self.balanceSheetFields = ['amended','audited','cashandcashequivalents','cashcashequivalentsandshortterminvestments','cashfromfinancingactivities','cashfrominvestingactivities','cashfromoperatingactivities','cik','commonstock','companyname','crosscalculated','currencycode','dcn','entityid','fiscalquarter','fiscalyear','formtype','inventoriesnet','marketoperator','markettier','original','otherassets','othercurrentassets','othercurrentliabilities','otherliabilities','periodenddate','periodlength','periodlengthcode','preliminary','primaryexchange','primarysymbol','propertyplantequipmentnet','receiveddate','restated','siccode','sicdescription','taxonomyid','totaladjustments','totalassets','totalcurrentassets','totalcurrentliabilities','totalliabilities','totallongtermdebt','totalreceivablesnet','totalshorttermdebt','totalstockholdersequity','usdconversionrate','goodwill','intangibleassets']


	def loadRaw(self,update=[],forceRefetch=False,ignore=[]):
		"""
		Load fundamentals and prices
		:return:
		"""

		# Load raw fundamentals
		path = os.path.relpath("{}/data/raw/fundamentals/{}.csv".format(dirname(dirname(dirname(__file__))),self.ticker))
		if (not os.path.isfile(path) and forceRefetch) or ('fundamentals' in update):
			print("<{}><Downloading fundamental data>".format(self.ticker))
			scraper = SECScraper(self.ticker)
			scraper.storeAllQuarterFilings()

		try:
			self.fundamentals = pd.read_csv(path,index_col='receiveddate', parse_dates=True).sort_values(by='receiveddate')
			# Add empty columns for all fields of which no data exists
			for field in self.balanceSheetFields+self.incomeStatementFields:
				if field not in self.fundamentals.columns and field != 'receiveddate':
					self.fundamentals[field] = np.nan
		except FileNotFoundError:
			if 'fundamentals' not in ignore: raise NoDataFoundException("No fundamental data found on EDGAR")

		# Load raw prices & shares outstanding
		pricePath = os.path.relpath("{}/data/raw/prices/{}.csv".format(dirname(dirname(dirname(__file__))),self.ticker))
		generalPath = os.path.relpath("{}/data/raw/general/{}.csv".format(dirname(dirname(dirname(__file__))),self.ticker))
		if (not os.path.isfile(pricePath) and forceRefetch) or (not os.path.isfile(generalPath) and forceRefetch) or ('prices' in update) or ('general' in update):
			print("<{}><Downloading price data>".format(self.ticker))
			scraper = YahooFinanceScraper(self.ticker)
			scraper.storeAllPrices()
			print("<{}><Downloading general data>".format(self.ticker))
			scraper.storeGeneral()

		try:
			self.prices = pd.read_csv(pricePath,index_col=0, parse_dates=True).sort_values(by='Date')
		except KeyError:
			if 'prices' not in ignore: raise NoDataFoundException("No price data found on Yahoo Finance")
		except FileNotFoundError:
			if 'prices' not in ignore: raise NoDataFoundException("No price data found on Yahoo Finance")

		try:
			with open(generalPath, 'r') as file:
				self.general = ast.literal_eval(file.readline())
				try:
					self.marketCap = self.general['quoteSummary']['result'][0]['summaryDetail']['marketCap']['raw']
					#self.sharesOutstanding = self.general['quoteSummary']['result'][0]['defaultKeyStatistics']['sharesOutstanding']['raw']
					self.sharesOutstanding = self.marketCap/self.general['quoteSummary']['result'][0]['financialData']['currentPrice']['raw']
				except KeyError:
					raise NoDataFoundException("No shares outstanding data found on Yahoo Finance...")
				except TypeError:
					raise NoDataFoundException("No data found on Yahoo Finance...")
		except FileNotFoundError:
			if 'general' not in ignore: raise NoDataFoundException("No general data found on Yahoo Finance")

	def getFundamentals(self):
		if self.fundamentals is None:
			raise Exception("Fundamentals weren't loaded...")
		return self.fundamentals

	def getPrices(self):
		if self.prices is None:
			raise Exception("Prices weren't loaded...")
		return self.prices

	def getSharesOutstanding(self):
		if self.sharesOutstanding is None:
			raise Exception("Shares outstanding weren't loaded...")
		return self.sharesOutstanding

	def getMarketCap(self):
		if self.marketCap is None:
			raise Exception("Market Cap wasn't loaded...")
		return self.marketCap

	def getGeneral(self):
		if self.general is None:
			raise Exception("General data wasn't loaded...")
		return self.general

	def getCleaned(self):
		if self.cleaned is None:
			raise Exception("Cleaned data wasn't loaded...")
		return self.cleaned

	def getLatestFundamentals(self, date, SECDelay, numberOfQuarters=4, gapFilling='fillout'):
		"""
		Returns a series object containing the combined fundamentals of the last numberOfQuarters quarters that were published BEFORE date
		(as reports are often released just after market closing).
		In case of balance sheet items, this means the latest numbers.
		In case of income statement items, this means the sum of the last numberOfQuarters numbers.
		In case of missing quarter statements, an exception is raised.
		:param date:
		:param numberOfQuarters:
		:return:
		"""

		if numberOfQuarters > 4:
			raise AttributeError("Number of quarters cannot exceed 4")

		if gapFilling not in ('interpolate','fillout'):
			raise AttributeError("Unknown gap-filling method")

		fundamentals = self.getFundamentals()

		availableRecords = fundamentals.loc[fundamentals.index <= date - datetime.timedelta(SECDelay), :]

		if len(availableRecords) == 0:
			self.lastLatestFundamentalsResult = [None, {'incomestatement':None, 'balancesheet':None}]
			return self.lastLatestFundamentalsResult[1]

		# Check if we already got the latest fundamentals and if so, return those
		lastReceivedDate = availableRecords.index[-1]
		if lastReceivedDate == self.lastLatestFundamentalsResult[0]:
			return self.lastLatestFundamentalsResult[1]

		self.lastLatestFundamentalsResult[0] = lastReceivedDate

		# Find most recent quarter filing
		latestFiscalYear = availableRecords['fiscalyear'].max()
		latestFiscalQuarter = availableRecords.loc[availableRecords.fiscalyear.isin((latestFiscalYear,)),:]['fiscalquarter'].max()

		## Generate Balance Sheet Datapoints
		balanceSheetDataFrame = availableRecords.loc[availableRecords.fiscalyear.isin((latestFiscalYear,))&availableRecords.fiscalquarter.isin((latestFiscalQuarter,)),:].sort_values(by='receiveddate',ascending=False).iloc[0,:]
		balanceSheetDataFrame = balanceSheetDataFrame[list(filter(lambda field: field != 'receiveddate', self.balanceSheetFields))]
		self.lastLatestFundamentalsResult[1]['balancesheet'] = balanceSheetDataFrame

		## Generate Income Statement Datapoints

		# Generate desired filings
		desiredFilings = [(math.floor((latestFiscalYear*4 + latestFiscalQuarter - i - 1)/4), (latestFiscalYear*4 + latestFiscalQuarter - i - 1)%4 + 1) for i in range(numberOfQuarters)]

		# Find out which filings we don't have
		gaps = list(filter(lambda filing: len(availableRecords.loc[availableRecords.fiscalyear.isin((filing[0],))&availableRecords.fiscalquarter.isin((filing[1],)),:])==0,desiredFilings))
		found = list(filter(lambda filing: filing not in gaps, desiredFilings))

		if gapFilling == 'interpolate':
			# Find previous-year values for gaps
			replacements = [availableRecords.loc[availableRecords.fiscalyear.isin((gap[0]-1,))&availableRecords.fiscalquarter.isin((gap[1],)),:] for gap in gaps]

			gapsDataFrame = pd.DataFrame()
			for replacement in replacements:
				if len(replacement) == 0:
					#raise Exception("Could not compute interpolation, as not enough data was available")
					return self.lastLatestFundamentalsResult[1]
				gapsDataFrame = gapsDataFrame.append(replacement,ignore_index=True)

			# Find previous-year values for found
			previous = {(gap[0], gap[1]):availableRecords.loc[availableRecords.fiscalyear.isin((gap[0]-1,))&availableRecords.fiscalquarter.isin((gap[1],)),:] for gap in found}

			ratesOfChange = pd.DataFrame()
			for currentYear, currentQuarter in previous.keys():
				previousValues = previous[(currentYear,currentQuarter)]
				currentValues = availableRecords.loc[availableRecords.fiscalyear.isin((currentYear,))&availableRecords.fiscalquarter.isin((currentQuarter,)),:]
				if len(previousValues) == 0:
					continue

				ratesOfChange = ratesOfChange.append((currentValues-previousValues)/previousValues,ignore_index=True)
				# TODO handle division by zero errors
				# TODO make sure that in case of duplicate fiscalyear-fiscalquarter pairs, most recent one is taken .sort_values(by='receiveddate',ascending=False).iloc[0,:]

			print(ratesOfChange)

		elif gapFilling == 'fillout':
			if len(gaps)/len(desiredFilings) > 1/4:
				#raise Exception("Could not compute fillout, as not enough data was available")
				return self.lastLatestFundamentalsResult[1]

			actualData = [availableRecords.loc[availableRecords.fiscalyear.isin((filing[0],))&availableRecords.fiscalquarter.isin((filing[1],)),:].sort_values(by='receiveddate',ascending=False).iloc[0,:] for filing in found]
			incomeStatementDataFrame = pd.DataFrame()
			for entry in actualData:
				incomeStatementDataFrame = incomeStatementDataFrame.append(entry,ignore_index=True)

			incomeStatementDataFrame = incomeStatementDataFrame[self.incomeStatementFields]
			incomeStatementDataFrame = incomeStatementDataFrame.sum()*(float(len(desiredFilings))/float(len(incomeStatementDataFrame)))

			self.lastLatestFundamentalsResult[1]['incomestatement'] = incomeStatementDataFrame

		return self.lastLatestFundamentalsResult[1]

	def loadCleaned(self, version, params, update=[]):

		# Load cleaned data
		fileExtension = ("{}_"*len(params)).format(*list(params.values()))[:-1]
		path = os.path.relpath("{}/data/cleaned/{}/{}_{}.csv".format(dirname(dirname(dirname(__file__))),version,self.ticker,fileExtension))
		if (not os.path.isfile(path)) or (len(update) > 0):
			print("<{}><Processing raw data>".format(self.ticker))
			processor = RawDataProcessor(self, update=update)
			processor.process(version=version,**params)
			processor.storeResult()

		self.cleaned = pd.read_csv(path,index_col='Date', parse_dates=True).sort_values(by='Date')

	def loadSnapshot(self, params, update=[]):
		processor = RawDataProcessor(self, update=update)
		return processor.generateSnapshot(**params)

	def getOverview(self,update=[]):
		# Load all available data
		self.loadRaw(update=update,forceRefetch=False,ignore=['fundamentals',])

		max_years = 5
		min_years = 1

		# Load most recent EUROPE & SEC data
		files = os.listdir("{}/data/cleaned/snapshot/".format(dirname(dirname(dirname(__file__)))))
		europe_file = sorted([file for file in files if "europe" in file])[-1]
		sec_file = sorted([file for file in files if "sec" in file])[-1]
		europe_data = pd.read_csv("{}/data/cleaned/snapshot/{}".format(dirname(dirname(dirname(__file__))),europe_file),index_col=1)
		sec_data = pd.read_csv("{}/data/cleaned/snapshot/{}".format(dirname(dirname(dirname(__file__))),sec_file),index_col=0)

		result = {}

		# Select general data
		result['name'] = self.general['quoteSummary']['result'][0]['quoteType']['longName']
		result['ticker'] = self.ticker
		result['summary'] = self.general['quoteSummary']['result'][0]['assetProfile']['longBusinessSummary']

		# Select multiples and performance data
		if self.fundamentals is None:
			# Fundamental data comes from Yahoo Finance
			result['currencies'] = {'exchange':europe_data['currencycode_exchange'].loc[self.ticker],'filings':europe_data['currencycode_earnings'].loc[self.ticker]}

			# Select multiples and metrics data
			result['multiples'] = {}
			result['industry'] = europe_data['industry'].loc[self.ticker]
			europe_data['ROE'] = europe_data['netIncome']/europe_data['totalStockholderEquity']
			europe_data['P/B'] = europe_data['market_cap']/europe_data['conversion_rate_earnings_to_exchange']/europe_data['totalStockholderEquity']
			fields = ['EV/EBITDA','P/E','P/B','ROE','Current Ratio','Div. Yld.','market_cap']
			peer_data = europe_data[europe_data.industry == result['industry']][fields]
			for field in fields:
				result['multiples'][field] = {'company':europe_data[field].loc[self.ticker], 'peers':peer_data[field].median()}
			result['multiples']['current_ratio'] = result['multiples'].pop('Current Ratio')
			result['multiples']['dividend_yield'] = result['multiples'].pop('Div. Yld.')

			# Select performance data
			fundamentals = self.general['quoteSummary']['result'][0]['incomeStatementHistory']['incomeStatementHistory']
			other_fundamentals = self.general['quoteSummary']['result'][0]['cashflowStatementHistory']['cashflowStatements']
			if len(fundamentals) < min_years: raise NoDataFoundException("We couldn't find enough historical fundamental data")
			years = min(max_years,len(fundamentals),len(other_fundamentals))

			def safe_get(report,field):
				if 'raw' in report[field].keys():
					return report[field]['raw']
				else:
					return 0

			performance = {'labels':[str(datetime.datetime.fromtimestamp(report['endDate']['raw']).year) for report in fundamentals[:years]]}
			performance['totalrevenue'] = [safe_get(report,'totalRevenue') for report in fundamentals[:years]]
			performance['EBITDA'] = [safe_get(fundamentals[i],'ebit')+safe_get(other_fundamentals[i],'depreciation') for i in range(years)]
			performance['netincome'] = [safe_get(report,'netIncome') for report in fundamentals[:years]]

			performance = pd.DataFrame(performance)

			result['performance'] = performance
		else:
			# Fundamental data comes from EDGAR
			result['currencies'] = {'exchange':'USD','filings':sec_data['currencycode'].loc[self.ticker]}
			fundamentals = self.fundamentals

			# Select multiples and metrics data
			result['multiples'] = {}
			result['industry'] = sec_data['sicdescription'].loc[self.ticker]
			sec_data['ROE'] = sec_data['netincome']/sec_data['totalstockholdersequity']
			sec_data['current_ratio'] = sec_data['totalcurrentassets']/sec_data['totalcurrentliabilities']
			sec_data['dividend_yield'] = -sec_data['dividendspaid']/sec_data['market_cap']
			fields = ['EV/EBITDA','P/E','P/B','ROE','current_ratio','dividend_yield','market_cap']
			peer_data = sec_data[sec_data.sicdescription == result['industry']][fields]
			for field in fields:
				result['multiples'][field] = {'company':sec_data[field].loc[self.ticker], 'peers':peer_data[field].median()}

			# Select performance data
			fundamentals['EBITDA'] = fundamentals['ebit']+fundamentals['cfdepreciationamortization']
			fields = ['totalrevenue','EBITDA','netincome']


			def select_4qs(df,year,quarter):
				pairs = []
				while len(pairs)<4:
					pairs.append((year,quarter))
					if quarter == 1:
						year -= 1
						quarter = 4
					else:
						quarter -= 1

				result = df[((df.fiscalyear==pairs[0][0]) & (df.fiscalquarter==pairs[0][1]))|
						  ((df.fiscalyear==pairs[1][0]) & (df.fiscalquarter==pairs[1][1]))|
						  ((df.fiscalyear==pairs[2][0]) & (df.fiscalquarter==pairs[2][1]))|
						  ((df.fiscalyear==pairs[3][0]) & (df.fiscalquarter==pairs[3][1]))]
				if len(result.index) < 4: raise NoDataFoundException("We couldn't find 4 consecutive quarter releases...")
				return result

			fundamentals = fundamentals.sort_values(by=['fiscalyear','fiscalquarter'],ascending=False)
			current_year = fundamentals.iloc[0]['fiscalyear']
			current_quarter = fundamentals.iloc[0]['fiscalquarter']
			pairs = [(current_year,current_quarter),]+[(current_year-i,4) for i in range(1,max_years)]
			fundamentals_by_year = []
			for pair in pairs:
				try:
					fundamentals_by_year.append(select_4qs(fundamentals,pair[0],pair[1]))
				except NoDataFoundException:
					if len(fundamentals_by_year) < min_years:
						raise NoDataFoundException("We couldn't find enough historical fundamental data...")
					else:
						break
			pairs = pairs[:len(fundamentals_by_year)]



			this_year_fundamentals = fundamentals_by_year[0]
			ttm = len(this_year_fundamentals[this_year_fundamentals.fiscalyear==current_year].index) < 4
			if ttm:
				ttm_string = " TTM"
			else:
				ttm_string = ""

			performance = {'labels':["{}{}".format(int(pairs[0][0]),ttm_string),]+[str(int(pair[0])) for pair in pairs[1:]]}
			for field in fields:
				performance[field] = [df[field].sum() for df in fundamentals_by_year]
			performance = pd.DataFrame(performance)

			result['performance'] = performance

		# Select price data
		result['prices'] = self.prices[self.prices.index>=(self.prices.index.max()-datetime.timedelta(365*5))]['adj_close']

		return result


#reader = DataReader('AFX.L')
#reader.loadRaw(update=['general',],ignore=['fundamentals',])
#print(reader.getMarketCap())

# Trigger data updates with the words:
#	- fundamentals
#	- prices
#	- cleaned

