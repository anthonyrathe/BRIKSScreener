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
		self.general = None
		self.cleaned = None

		self.lastLatestFundamentalsResult = [None, {'incomestatement':None, 'balancesheet':None}] # Last received date, result

		self.incomeStatementFields = ['capitalexpenditures','cfdepreciationamortization','changeinaccountsreceivable','changeincurrentassets','changeincurrentliabilities','changeininventories','costofrevenue','dividendspaid','ebit','grossprofit','incomebeforetaxes','investmentchangesnet','netchangeincash','netincome','netincomeapplicabletocommon','researchdevelopmentexpense','retainedearnings','sellinggeneraladministrativeexpenses','totalrevenue']
		self.balanceSheetFields = ['amended','audited','cashandcashequivalents','cashcashequivalentsandshortterminvestments','cashfromfinancingactivities','cashfrominvestingactivities','cashfromoperatingactivities','cik','commonstock','companyname','crosscalculated','currencycode','dcn','entityid','fiscalquarter','fiscalyear','formtype','inventoriesnet','marketoperator','markettier','original','otherassets','othercurrentassets','othercurrentliabilities','otherliabilities','periodenddate','periodlength','periodlengthcode','preliminary','primaryexchange','primarysymbol','propertyplantequipmentnet','receiveddate','restated','siccode','sicdescription','taxonomyid','totaladjustments','totalassets','totalcurrentassets','totalcurrentliabilities','totalliabilities','totallongtermdebt','totalreceivablesnet','totalshorttermdebt','totalstockholdersequity','usdconversionrate','goodwill','intangibleassets']


	def loadRaw(self,update=[],forceRefetch=False):
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

		self.fundamentals = pd.read_csv(path,index_col='receiveddate', parse_dates=True).sort_values(by='receiveddate')

		# Add empty columns for all fields of which no data exists
		for field in self.balanceSheetFields+self.incomeStatementFields:
			if field not in self.fundamentals.columns:
				self.fundamentals[field] = np.nan

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
			raise NoDataFoundException("No price data found on Yahoo Finance")

		with open(generalPath, 'r') as file:
			self.general = ast.literal_eval(file.readline())
			try:
				self.sharesOutstanding = self.general['quoteSummary']['result'][0]['defaultKeyStatistics']['sharesOutstanding']['raw']
			except KeyError:
				raise NoDataFoundException("No shares outstanding data found on Yahoo Finance...")
			except TypeError:
				raise NoDataFoundException("No data found on Yahoo Finance...")

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


# Trigger data updates with the words:
#	- fundamentals
#	- prices
#	- cleaned