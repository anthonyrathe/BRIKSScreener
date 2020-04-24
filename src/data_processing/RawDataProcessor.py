import pandas as pd
import numpy as np
import os
from os.path import dirname as dirname
from tsfresh import extract_relevant_features, extract_features

class RawDataProcessor:

	def __init__(self, reader, update=[]):
		self.reader = reader
		try:
			self.reader.loadRaw(update=update,forceRefetch=False)
		except Exception:
			raise Exception("Something went wrong when obtaining the raw data...")
		self.result = None
		self.version = None
		self.fileName = None

	def process(self, version, **kwargs):

		if version == 'v1':
			return self.processV1(**kwargs)
		elif version == 'v2':
			return self.processV2(**kwargs)
		elif version == 'overview':
			return self.generateFullOverview(**kwargs)

	def processV1(self, trendMALength=10, trendMinimumGain=0.03, trendMinimumRelGain = 0.8, SECDelay=1):

		self.version = 'v1'
		self.trendMALength = trendMALength
		self.trendMinimumGain = trendMinimumGain
		self.trendMinimumRelGain = trendMinimumRelGain
		self.SECDelay = SECDelay
		self.fileName = "{}_{}_{}_{}_{}".format(self.reader.ticker,trendMALength,trendMinimumGain,trendMinimumRelGain,SECDelay)


		if trendMALength <= 0:
			raise Exception("trendMALenght must be larger than zero!")

		priceData = self.reader.getPrices()

		processedData = priceData.copy()
		processedData['Date'] = processedData.index

		# Calculate MA of close prices

		#	Ensure trendMALength is odd
		if trendMALength%2 == 0:
			trendMALength += 1

		processedData['MATrend'] = processedData['adj_close'].rolling(trendMALength).mean()
		processedData['MATrend'] = processedData['MATrend'].shift(-int((trendMALength-1)/2))

		# Find local minima and maxima of MATrend
		processedData['MATrendMin'] = processedData.MATrend[(processedData.MATrend.shift(1) >= processedData.MATrend) & (processedData.MATrend.shift(-1) > processedData.MATrend)]
		processedData['MATrendMax'] = processedData.MATrend[(processedData.MATrend.shift(1) <= processedData.MATrend) & (processedData.MATrend.shift(-1) < processedData.MATrend)]

		# Find dates of local minima and maxima of adj_close and interpolate with nearest
		processedData['adj_closeMin'] = processedData.adj_close[(processedData.adj_close.shift(1) >= processedData.adj_close) & (processedData.adj_close.shift(-1) > processedData.adj_close)]
		processedData['adj_closeMax'] = processedData.adj_close[(processedData.adj_close.shift(1) <= processedData.adj_close) & (processedData.adj_close.shift(-1) < processedData.adj_close)]
		processedData['adj_closeMinDate'] = processedData.dropna(subset=['adj_closeMin',])['Date']
		processedData['adj_closeMaxDate'] = processedData.dropna(subset=['adj_closeMax',])['Date']
		processedData['adj_closeNearestMinDate'] = pd.to_numeric(processedData['adj_closeMinDate']).replace(-9223372036854775808, np.nan)
		processedData['adj_closeNearestMinDate'] = pd.to_datetime(processedData['adj_closeNearestMinDate'].interpolate(method='nearest').fillna(method='bfill').fillna(method='ffill'))
		processedData['adj_closeNearestMaxDate'] = pd.to_numeric(processedData['adj_closeMaxDate']).replace(-9223372036854775808, np.nan)
		processedData['adj_closeNearestMaxDate'] = pd.to_datetime(processedData['adj_closeNearestMaxDate'].interpolate(method='nearest').fillna(method='bfill').fillna(method='ffill'))

		# Match local minima and maxima of MATrend with nearest local minima and maxima of adj_close
		processedData['MATrendMin'] = processedData.loc[processedData.dropna(subset=['MATrendMin',])['adj_closeNearestMinDate'].drop_duplicates(),:]['adj_closeMin']
		processedData['MATrendMax'] = processedData.loc[processedData.dropna(subset=['MATrendMax',])['adj_closeNearestMaxDate'].drop_duplicates(),:]['adj_closeMax']

		# TODO: Find dates at which Trend data could have been obtained
		processedData['turningPoints'] = pd.concat([processedData['MATrendMax'].dropna(), processedData['MATrendMin'].dropna()]).reindex_like(processedData)
		processedData['turningPointDate'] = pd.to_numeric(processedData.dropna(subset=['turningPoints',])['Date'].shift(-1)).replace(-9223372036854775808, np.nan)
		processedData['turningPointDate'] = pd.to_datetime(processedData['turningPointDate'].fillna(method='bfill')).dropna()

		# This piece of code allows us to see how long a trend lasts
		#import itertools
		#processedData['trendLength'] = pd.concat([processedData['MATrendMax'].dropna(), processedData['MATrendMin'].dropna()]).reindex_like(processedData)
		#len_holes = pd.Series([len(list(g)) for k, g in itertools.groupby(processedData['trendLength'], lambda x: np.isnan(x)) if k])
		#print(len_holes.describe())

		# Replace MATrend with adj_close
		processedData['MATrend'] = processedData['adj_close']

		# Calculate max gains:
		#	1. MATrendMaxBuyGain = (MATrendMaxNext - MATrendMaxGainCurrentMin)/MATrendMaxGainCurrentMin
		#	2. MATrendMaxSellGain = (MATrendMaxGainCurrentMax - MATrendMinNext)/MATrendMaxGainCurrentMax

		#	Backward fill MATrendMax
		processedData['MATrendMaxNext'] = processedData.MATrendMax.fillna(method='bfill')
		#	Backward fill MATrendMin
		processedData['MATrendMinNext'] = processedData.MATrendMin.fillna(method='bfill')

		global latestFieldValue
		latestFieldValue = None
		def returnLatest(notNanValue, fieldValue):
			global latestFieldValue
			if not np.isnan(fieldValue):
				latestFieldValue = fieldValue
			if not np.isnan(notNanValue):
				return latestFieldValue
			else:
				return None
		#	Find MATrendMaxGainCurrentMin for every maximum (i.e. latest min)
		processedData['MATrendMaxGainCurrentMin'] = processedData.apply(lambda x: returnLatest(x['MATrendMax'], x['MATrendMin']), axis=1).fillna(method='bfill')
		#	Find MATrendMaxGainCurrentMax for every minimum (i.e. latest max)
		latestFieldValue = None
		processedData['MATrendMaxGainCurrentMax'] = processedData.apply(lambda x: returnLatest(x['MATrendMin'], x['MATrendMax']), axis=1).fillna(method='bfill')

		#	Calculate max gains
		processedData['MATrendMaxBuyGain'] = (processedData['MATrendMaxNext']-processedData['MATrendMaxGainCurrentMin'])/processedData['MATrendMaxGainCurrentMin']
		processedData['MATrendMaxSellGain'] = (processedData['MATrendMaxGainCurrentMax']-processedData['MATrendMinNext'])/processedData['MATrendMaxGainCurrentMax']

		# Calculate gains:
		#	1. MATrendBuyGain = (MATrendMaxNext - adj_close)/MATrend
		#	2. MATrendSellGain = (MATrend - MATrendMinNext)/MATrend
		processedData['MATrendBuyGain'] = (processedData['MATrendMaxNext']-processedData['MATrend'])/processedData['MATrend']
		processedData['MATrendSellGain'] = (processedData['MATrend']-processedData['MATrendMinNext'])/processedData['MATrend']

		# Calculate relative gains:
		#	1. MATrendRelBuyGain = MATrendBuyGain/MATrendMaxBuyGain
		#	2. MATrendRelSellGain = MATrendSellGain/MATrendMaxSellGain
		processedData['MATrendRelBuyGain'] = processedData['MATrendBuyGain']/processedData['MATrendMaxBuyGain']
		processedData['MATrendRelSellGain'] = processedData['MATrendSellGain']/processedData['MATrendMaxSellGain']

		# Trends:
		#	SELL: 		-1
		#	NEUTRAL:	0
		#	BUY:		1

		# Calculate dominant trend:
		#	if MATrendBuyGain >= MATrendSellGain:
		#		Trend = BUY
		#		TrendGain = MATrendBuyGain
		#		TrendRelGain = MATrendRelBuyGain
		#	elif MATrendSellGain > MATrensBuyGain (else)
		#		Trend = SELL
		#		TrendGain = MATrendSellGain
		#		TrendRelGain = MATrendRelSellGain
		processedData['Trend'] = processedData.apply(lambda x: 'BUY' if x['MATrendBuyGain'] >= x['MATrendSellGain'] else 'SELL', axis=1)
		processedData['TrendGain'] = processedData.apply(lambda x: x['MATrendBuyGain'] if x['MATrendBuyGain'] >= x['MATrendSellGain'] else x['MATrendSellGain'], axis=1)
		processedData['TrendRelGain'] = processedData.apply(lambda x: x['MATrendRelBuyGain'] if x['MATrendBuyGain'] >= x['MATrendSellGain'] else x['MATrendRelSellGain'], axis=1)

		# Eliminate neutral trends and narrow down window of opportunity
		#	if TrendGain < trendMinimumGain or TrendRelGain < trendMinimumRelGain:
		#		Trend = NEUTRAL
		processedData['Trend'] = processedData.apply(lambda x: 'NEUTRAL' if (x['TrendGain'] < trendMinimumGain or x['TrendRelGain'] < trendMinimumRelGain) else x['Trend'], axis=1)

		# Generate Fundamentals: EV/EBITDA, P/E, P/B, D/E
		# 	Load in:
		#		- ebit
		#		- cfdepreciationamortization
		#		- totalliabilities
		#		- cashandcashequivalents
		#		- netincomeapplicabletocommon
		#		- totalstockholdersequity
		#		- totalrevenue
		def getLatestFundamentals(date):
			fundamentals = self.reader.getLatestFundamentals(date,self.SECDelay)
			incomeStatement = fundamentals['incomestatement']
			balanceSheet = fundamentals['balancesheet']
			fields = self.reader.incomeStatementFields + self.reader.balanceSheetFields
			result = dict.fromkeys(fields)

			if incomeStatement is not None:
				result.update(incomeStatement)
			if balanceSheet is not None:
				result.update(balanceSheet)

			return result

		processedData = processedData.merge(processedData.Date.apply(lambda d: pd.Series(getLatestFundamentals(d))),left_index=True,right_index=True)


		processedData['market_cap'] = processedData['adj_close']*self.reader.getSharesOutstanding()
		processedData['EBITDA'] = processedData['ebit']+processedData['cfdepreciationamortization']

		processedData['EV/EBITDA'] = (processedData['market_cap']+processedData['totalliabilities']-processedData['cashandcashequivalents'])/processedData['EBITDA']
		processedData['P/E'] = processedData['market_cap']/processedData['netincomeapplicabletocommon']
		processedData['P/B'] = processedData['market_cap']/processedData['totalstockholdersequity']
		processedData['D/E'] = processedData['totalliabilities']/processedData['totalstockholdersequity']

		# Generate derived fundamentals:
		#	- Eearnings growth (1y)
		#	- EBITDA growth (1y)
		#	- Revenue growth (1y)
		#	- Volume growth (1y)
		nbOfTradingDays = 252
		processedData['revenueGrowth1y'] = processedData['totalrevenue'].pct_change(periods=nbOfTradingDays*1)
		processedData['EBITDAGrowth1y'] = processedData['EBITDA'].pct_change(periods=nbOfTradingDays*1)
		processedData['earningsGrowth1y'] = processedData['netincomeapplicabletocommon'].pct_change(periods=nbOfTradingDays*1)
		processedData['volumeGrowth1y'] = processedData['volume'].pct_change(periods=nbOfTradingDays*1)

		# Generate Technicals: R, Bias, SMA, EMA, ...
		#	- adj_close: RSI, R, Bias, (EMA-SMA)/SMA
		#	- EV/EBITDA: R, Bias, (EMA-SMA)/SMA
		#	- P/E: R, Bias, (EMA-SMA)/SMA
		#	- P/B: R, Bias, (EMA-SMA)/SMA
		#	- D/E: R, Bias, (EMA-SMA)/SMA
		#	- volume: R, Bias, SMA, EMA, (EMA-SMA)/SMA

		#	RSI (14)
		RSI_N = 14
		processedData['priceChange'] = processedData['adj_close'].diff(periods=1)
		processedData['priceGain'] = processedData.loc[processedData.priceChange >= 0,:]['priceChange']
		processedData['priceLoss'] = -processedData.loc[processedData.priceChange < 0,:]['priceChange']
		processedData['priceGain'] = processedData['priceGain'].fillna(0)
		processedData['priceLoss'] = processedData['priceLoss'].fillna(0)
		processedData['avgPriceGain'] = processedData['priceGain'].rolling(RSI_N).mean()
		processedData['avgPriceLoss'] = processedData['priceLoss'].rolling(RSI_N).mean()
		processedData['RS'] = processedData['avgPriceGain']/processedData['avgPriceLoss']
		processedData['RSI'] = processedData['RS'].apply(lambda x: 1 - 1/(1+x))

		#	R
		# TODO: EV/EBITDA, P/E en P/B should be recalculated for high and low values of price...
		R_arguments = (	('Price', 		'high', 		'low', 			'adj_close', 	28),
						('EV/EBITDA', 	'EV/EBITDA', 	'EV/EBITDA', 	'EV/EBITDA', 	60),
						('P/E',			'P/E',			'P/E',			'P/E', 			60),
						('P/B',			'P/B',			'P/B',			'P/B', 			60),
						('D/E',			'D/E',			'D/E',			'D/E', 			nbOfTradingDays),
						('Volume',		'volume',		'volume',		'volume', 		28))
		for (name, high, low, current, R_N) in R_arguments:
			processedData['max{}'.format(name)] = processedData[high].rolling(R_N).max()
			processedData['min{}'.format(name)] = processedData[low].rolling(R_N).min()
			processedData['R_{}'.format(name)] = (processedData['max{}'.format(name)]-processedData[current])/(processedData['max{}'.format(name)]-processedData['min{}'.format(name)])

		#	Bias
		Bias_arguments = (	('Price',		'adj_close',		16),
							('Price',		'adj_close',		28),
							('EV/EBITDA',	'EV/EBITDA',		60),
							('P/E',			'P/E',				60),
							('P/B',			'P/B',				60),
							('D/E',			'D/E',				nbOfTradingDays),
							('Volume',		'volume', 			28))
		for (name, current, Bias_N) in Bias_arguments:
			processedData['SMA_{}_{}'.format(name,Bias_N)] = processedData[current].rolling(Bias_N).mean()
			processedData['Bias_{}_{}'.format(name,Bias_N)] = (processedData[current]-processedData['SMA_{}_{}'.format(name,Bias_N)])/processedData['SMA_{}_{}'.format(name,Bias_N)]

		# Exponential Bias
		EBias_arguments = (	('Price',		'adj_close',		16, 	10),
							('Price',		'adj_close',		28,		16),
							('EV/EBITDA',	'EV/EBITDA',		60,		35),
							('P/E',			'P/E',				60,		35),
							('P/B',			'P/B',				60,		35),
							('D/E',			'D/E',				nbOfTradingDays,	int(nbOfTradingDays*0.6)),
							('Volume',		'volume', 			28,		16))
		for (name, current, SMA_N, EMA_N) in EBias_arguments:
			if 'SMA_{}_{}'.format(name,SMA_N) not in processedData.columns:
				processedData['SMA_{}_{}'.format(name,SMA_N)] = processedData[current].rolling(SMA_N).mean()
			processedData['EMA_{}_{}'.format(name,EMA_N)] = processedData[current].ewm(span=EMA_N).mean()
			processedData['EBias_{}_{}_{}'.format(name,SMA_N,EMA_N)] = (processedData['EMA_{}_{}'.format(name,EMA_N)]-processedData['SMA_{}_{}'.format(name,SMA_N)])/processedData['SMA_{}_{}'.format(name,SMA_N)]

		processedData['label'] = processedData['Trend']

		self.result = processedData
		return self.result

	def processV2(self, SECDelay, price_delay, fix_received_dates, fix_missing_data):
		# TODO: note that we assume that all fundamental data is in USD!!!!! -> should add conversion to USD
		# SEC Delay: if SEC Delay == 1, this means that we can ACT on SEC data only the day AFTER filing
		# price delay: if price delay == 1, this means that we can ACT on price data only the day after the price data was given
		# The index corresponds to the date at which we ACT (i.e. buy or sell)
		self.version = 'v2'
		self.SECDelay = SECDelay
		if not fix_received_dates:
			self.fileName = "{}_{}_{}_not_fixed".format(self.reader.ticker,SECDelay,price_delay)
		else:
			self.fileName = "{}_{}_{}".format(self.reader.ticker,SECDelay,price_delay)

		if fix_received_dates: self.reader.fixFundamentalsReceivedDate()
		if fix_missing_data: self.reader.fixFundamentalsMissingData()

		priceData = self.reader.getPrices()

		processedData = priceData.copy()
		processedData['open'] = processedData['open'].shift(price_delay)
		processedData['high'] = processedData['high'].shift(price_delay)
		processedData['low'] = processedData['low'].shift(price_delay)
		processedData['close'] = processedData['close'].shift(price_delay)
		processedData['adj_close'] = processedData['adj_close'].shift(price_delay)
		processedData['volume'] = processedData['volume'].shift(price_delay)
		processedData['Date'] = processedData.index
		processedData.dropna(inplace=True)

		# Generate Fundamentals: EV/EBITDA, P/E, P/B, D/E
		# 	Load in:
		#		- ebit
		#		- cfdepreciationamortization
		#		- totalliabilities
		#		- cashandcashequivalents
		#		- netincomeapplicabletocommon
		#		- totalstockholdersequity
		#		- totalrevenue
		def getLatestFundamentals(date):
			fundamentals = self.reader.getLatestFundamentals(date,self.SECDelay)
			incomeStatement = fundamentals['incomestatement']
			balanceSheet = fundamentals['balancesheet']
			fields = self.reader.incomeStatementFields + self.reader.balanceSheetFields
			result = dict.fromkeys(fields)

			if incomeStatement is not None:
				result.update(incomeStatement)
			if balanceSheet is not None:
				result.update(balanceSheet)

			return result

		processedData = processedData.merge(processedData.Date.apply(lambda d: pd.Series(getLatestFundamentals(d))),left_index=True,right_index=True)


		processedData['market_cap'] = processedData['adj_close']*self.reader.getSharesOutstanding()
		processedData['EBITDA'] = processedData['ebit']+processedData['cfdepreciationamortization']

		processedData['EV/EBITDA'] = (processedData['market_cap']+processedData['totalliabilities']-processedData['cashandcashequivalents'])/processedData['EBITDA']
		processedData['P/E'] = processedData['market_cap']/processedData['netincomeapplicabletocommon']
		processedData['P/B'] = processedData['market_cap']/processedData['totalstockholdersequity']
		processedData['D/E'] = processedData['totalliabilities']/processedData['totalstockholdersequity']

		# Generate derived fundamentals:
		#	- Eearnings growth (1y)
		#	- EBITDA growth (1y)
		#	- Revenue growth (1y)
		#	- Volume growth (1y)
		nbOfTradingDays = 252
		processedData['revenueGrowth1y'] = processedData['totalrevenue'].pct_change(periods=nbOfTradingDays*1)
		processedData['EBITDAGrowth1y'] = processedData['EBITDA'].pct_change(periods=nbOfTradingDays*1)
		processedData['earningsGrowth1y'] = processedData['netincomeapplicabletocommon'].pct_change(periods=nbOfTradingDays*1)
		processedData['volumeGrowth1y'] = processedData['volume'].pct_change(periods=nbOfTradingDays*1)

		# Generate Technicals: R, Bias, SMA, EMA, ...
		#	- adj_close: RSI, R, Bias, (EMA-SMA)/SMA
		#	- EV/EBITDA: R, Bias, (EMA-SMA)/SMA
		#	- P/E: R, Bias, (EMA-SMA)/SMA
		#	- P/B: R, Bias, (EMA-SMA)/SMA
		#	- D/E: R, Bias, (EMA-SMA)/SMA
		#	- volume: R, Bias, SMA, EMA, (EMA-SMA)/SMA

		#	RSI (14)
		RSI_N = 14
		processedData['priceChange'] = processedData['adj_close'].diff(periods=1)
		processedData['priceGain'] = processedData.loc[processedData.priceChange >= 0,:]['priceChange']
		processedData['priceLoss'] = -processedData.loc[processedData.priceChange < 0,:]['priceChange']
		processedData['priceGain'] = processedData['priceGain'].fillna(0)
		processedData['priceLoss'] = processedData['priceLoss'].fillna(0)
		processedData['avgPriceGain'] = processedData['priceGain'].rolling(RSI_N).mean()
		processedData['avgPriceLoss'] = processedData['priceLoss'].rolling(RSI_N).mean()
		processedData['RS'] = processedData['avgPriceGain']/processedData['avgPriceLoss']
		processedData['RSI'] = processedData['RS'].apply(lambda x: 1 - 1/(1+x))

		#	R
		# TODO: EV/EBITDA, P/E en P/B should be recalculated for high and low values of price...
		R_arguments = (	('Price', 		'high', 		'low', 			'adj_close', 	28),
						('EV/EBITDA', 	'EV/EBITDA', 	'EV/EBITDA', 	'EV/EBITDA', 	60),
						('P/E',			'P/E',			'P/E',			'P/E', 			60),
						('P/B',			'P/B',			'P/B',			'P/B', 			60),
						('D/E',			'D/E',			'D/E',			'D/E', 			nbOfTradingDays),
						('Volume',		'volume',		'volume',		'volume', 		28))
		for (name, high, low, current, R_N) in R_arguments:
			processedData['max{}'.format(name)] = processedData[high].rolling(R_N).max()
			processedData['min{}'.format(name)] = processedData[low].rolling(R_N).min()
			processedData['R_{}'.format(name)] = (processedData['max{}'.format(name)]-processedData[current])/(processedData['max{}'.format(name)]-processedData['min{}'.format(name)])

		#	Bias
		Bias_arguments = (	('Price',		'adj_close',		16),
							('Price',		'adj_close',		28),
							('EV/EBITDA',	'EV/EBITDA',		60),
							('P/E',			'P/E',				60),
							('P/B',			'P/B',				60),
							('D/E',			'D/E',				nbOfTradingDays),
							('Volume',		'volume', 			28))
		for (name, current, Bias_N) in Bias_arguments:
			processedData['SMA_{}_{}'.format(name,Bias_N)] = processedData[current].rolling(Bias_N).mean()
			processedData['Bias_{}_{}'.format(name,Bias_N)] = (processedData[current]-processedData['SMA_{}_{}'.format(name,Bias_N)])/processedData['SMA_{}_{}'.format(name,Bias_N)]

		# Exponential Bias
		EBias_arguments = (	('Price',		'adj_close',		16, 	10),
							('Price',		'adj_close',		28,		16),
							('EV/EBITDA',	'EV/EBITDA',		60,		35),
							('P/E',			'P/E',				60,		35),
							('P/B',			'P/B',				60,		35),
							('D/E',			'D/E',				nbOfTradingDays,	int(nbOfTradingDays*0.6)),
							('Volume',		'volume', 			28,		16))
		for (name, current, SMA_N, EMA_N) in EBias_arguments:
			if 'SMA_{}_{}'.format(name,SMA_N) not in processedData.columns:
				processedData['SMA_{}_{}'.format(name,SMA_N)] = processedData[current].rolling(SMA_N).mean()
			processedData['EMA_{}_{}'.format(name,EMA_N)] = processedData[current].ewm(span=EMA_N).mean()
			processedData['EBias_{}_{}_{}'.format(name,SMA_N,EMA_N)] = (processedData['EMA_{}_{}'.format(name,EMA_N)]-processedData['SMA_{}_{}'.format(name,SMA_N)])/processedData['SMA_{}_{}'.format(name,SMA_N)]

		self.result = processedData
		return self.result

	def addTimeSeriesFeatures(self):
		data = self.result
		data['id'] = 1
		data = data.dropna(axis=1)
		#y = data['close']
		#extracted_features = extract_relevant_features(data, y, column_id="id", column_sort="Date")
		extracted_features = extract_features(data, column_id="id", column_sort="Date")
		self.result = extracted_features
		return self.result



	def generateSnapshot(self, SECDelay=2):

		processedData = self.generateFullOverview(SECDelay=SECDelay)
		self.result = processedData.iloc[-1,:]
		return self.result

	def generateFullOverview(self, SECDelay=0, ticker=None):
		self.SECDelay = SECDelay
		self.fileName = ticker
		self.version = 'overview'

		priceData = self.reader.getPrices()
		nbOfTradingDays = 252

		processedData = priceData.iloc[-nbOfTradingDays-2:-1,:].copy()
		processedData['Date'] = processedData.index

		# Generate Fundamentals: EV/EBITDA, P/E, P/B, D/E
		# 	Load in:
		#		- ebit
		#		- cfdepreciationamortization
		#		- totalliabilities
		#		- cashandcashequivalents
		#		- netincomeapplicabletocommon
		#		- totalstockholdersequity
		#		- totalrevenue
		def getLatestFundamentals(date):
			fundamentals = self.reader.getLatestFundamentals(date,self.SECDelay)
			incomeStatement = fundamentals['incomestatement']
			balanceSheet = fundamentals['balancesheet']
			fields = self.reader.incomeStatementFields + self.reader.balanceSheetFields
			result = dict.fromkeys(fields)

			if incomeStatement is not None:
				result.update(incomeStatement)
			if balanceSheet is not None:
				result.update(balanceSheet)

			return result

		processedData = processedData.merge(processedData.Date.apply(lambda d: pd.Series(getLatestFundamentals(d))),left_index=True,right_index=True)


		processedData['market_cap'] = processedData['adj_close']*self.reader.getSharesOutstanding()
		#processedData['market_cap'] = self.reader.getMarketCap()
		processedData['EBITDA'] = processedData['ebit']+processedData['cfdepreciationamortization']

		processedData['EV/EBITDA'] = (processedData['market_cap']/processedData['usdconversionrate']+processedData['totalliabilities']-processedData['cashandcashequivalents'])/processedData['EBITDA']
		processedData['P/E'] = (processedData['market_cap']/processedData['usdconversionrate'])/processedData['netincomeapplicabletocommon']
		processedData['P/B'] = (processedData['market_cap']/processedData['usdconversionrate'])/processedData['totalstockholdersequity']
		processedData['D/E'] = processedData['totalliabilities']/processedData['totalstockholdersequity']

		# Generate derived fundamentals:
		#	- Eearnings growth (1y)
		#	- EBITDA growth (1y)
		#	- Revenue growth (1y)
		#	- Volume growth (1y)
		processedData['revenueGrowth1y'] = processedData['totalrevenue'].pct_change(periods=nbOfTradingDays*1)
		processedData['EBITDAGrowth1y'] = processedData['EBITDA'].pct_change(periods=nbOfTradingDays*1)
		processedData['earningsGrowth1y'] = processedData['netincomeapplicabletocommon'].pct_change(periods=nbOfTradingDays*1)
		processedData['volumeGrowth1y'] = processedData['volume'].pct_change(periods=nbOfTradingDays*1)

		self.result = processedData
		return self.result



	def storeResult(self):

		if self.result is None:
			raise Exception("Data hasn't been processed...")

		path = os.path.relpath("{}/data/cleaned/{}/{}.csv".format(dirname(dirname(dirname(__file__))),self.version,self.fileName))
		self.result.to_csv(path,index=True)

