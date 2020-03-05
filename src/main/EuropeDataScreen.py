from src.data_collecting.YahooFinanceScraper import YahooFinanceScraper
from src.data_processing.TickerLoader import TickerLoader
import pandas as pd
import os
import datetime
from os.path import dirname as dirname

exchanges = ['frankfurt','euronext','lse']

def generateScreen(exchange):
	loader = TickerLoader()
	loader.scrapeTickers(exchange)
	tickers = loader.getTickers(exchange,filterTickers=False,exchangeSuffix=True)

	fields = ['conversion_rate_earnings_to_exchange', 'currencycode_earnings', 'currencycode_exchange', 'industry', 'sector', 'EBITDA', 'earningsGrowth1y', 'revenueGrowth1y', 'EBITDAGrowth1y', 'name', 'strongBuy', 'buy', 'hold', 'sell', 'strongSell', 'endDate', 'market_cap', 'accountsPayable', 'capitalSurplus', 'cash', 'commonStock', 'deferredLongTermLiab', 'goodWill', 'intangibleAssets', 'longTermDebt', 'longTermInvestments', 'netReceivables', 'netTangibleAssets', 'otherAssets', 'otherCurrentAssets', 'otherCurrentLiab', 'otherLiab', 'otherStockholderEquity', 'propertyPlantEquipment', 'retainedEarnings', 'shortLongTermDebt', 'shortTermInvestments', 'totalAssets', 'totalCurrentAssets', 'totalCurrentLiabilities', 'totalLiab', 'totalStockholderEquity', 'treasuryStock', 'capitalExpenditures', 'changeInCash', 'changeToLiabilities', 'changeToNetincome', 'changeToOperatingActivities', 'depreciation', 'dividendsPaid', 'effectOfExchangeRate', 'investments', 'issuanceOfStock', 'netBorrowings', 'netIncome', 'repurchaseOfStock', 'totalCashFromFinancingActivities', 'totalCashFromOperatingActivities', 'totalCashflowsFromInvestingActivities', 'costOfRevenue', 'discontinuedOperations', 'ebit', 'effectOfAccountingCharges', 'extraordinaryItems', 'grossProfit', 'incomeBeforeTax', 'incomeTaxExpense', 'interestExpense', 'minorityInterest', 'netIncomeApplicableToCommonShares', 'netIncomeFromContinuingOps', 'nonRecurring', 'operatingIncome', 'otherItems', 'otherOperatingExpenses', 'researchDevelopment', 'sellingGeneralAdministrative', 'totalOperatingExpenses', 'totalOtherIncomeExpenseNet', 'totalRevenue', 'deferredLongTermAssetCharges', 'inventory', 'changeToAccountReceivables', 'changeToInventory', 'otherCashflowsFromFinancingActivities', 'otherCashflowsFromInvestingActivities', 'price']

	result = pd.DataFrame(columns=fields)
	length = len(tickers)
	count = 0
	for ticker in tickers[:10]:
		count += 1
		print("{} -- {}%".format(ticker, count/length*100))
		scraper = YahooFinanceScraper(ticker)
		snapshot = scraper.getSnapshot()

		if snapshot is not None:

			def fieldValue(field):
				try:
					return snapshot[field]
				except KeyError:
					return None

			snapshot_series = pd.Series({field:fieldValue(field) for field in fields},name=ticker)
			result = result.append(snapshot_series)

	result.index.name = 'symbol'

	date = datetime.date.today()
	path = os.path.relpath("{}/data/cleaned/snapshot/{}_{}.csv".format(dirname(dirname(dirname(__file__))),exchange,str(date)))
	result.to_csv(path,index=True)


for exchange in exchanges:
	generateScreen(exchange)

europe = None
date = datetime.date.today()
for exchange in exchanges:
	path = os.path.relpath("{}/data/cleaned/snapshot/{}_{}.csv".format(dirname(dirname(dirname(__file__))),exchange,str(date)))
	total = pd.read_csv(path)

	if europe is None:
		europe = total
	else:
		europe = pd.concat((europe,total),axis=0,ignore_index=True)

europe = europe.rename({'endDate':'date_of_last_filing'},axis=1)
europe['NNWC Ratio'] = (europe['market_cap']/europe['conversion_rate_earnings_to_exchange'])/(europe['totalCurrentAssets']-europe['totalLiab'])
europe['EV/EBITDA']=((europe['market_cap']/europe['conversion_rate_earnings_to_exchange'])+europe['totalLiab']-europe['cash'])/(europe['EBITDA'])
europe['P/E'] = (europe['market_cap']/europe['conversion_rate_earnings_to_exchange'])/europe['netIncome']
europe['Div. Yld.'] = -europe['dividendsPaid']/(europe['market_cap']/europe['conversion_rate_earnings_to_exchange'])
europe['Current Ratio'] = -europe['ebit']/europe['interestExpense']
europe['FCF'] = europe['totalCashFromOperatingActivities']-europe['capitalExpenditures']

def symbol_to_exchange(symbol):
	if symbol[-2:] == ".F":
		return 'Frankfurt'
	elif symbol[-2:] == ".L":
		return 'London Stock Exchange'
	elif symbol[-3:] == ".PA":
		return 'Euronext Paris'
	elif symbol[-3:] == ".BR":
		return 'Euronext Brussels'
	elif symbol[-3:] == ".AS":
		return 'Euronext Amsterdam'
	elif symbol[-3:] == ".IR":
		return 'Euronext Dublin'
	elif symbol[-3:] == ".LS":
		return 'Euronext Lisbon'
	else:
		return 'Unknown'

europe['exchange'] = europe['symbol'].apply(lambda symbol: symbol_to_exchange(symbol))

path = os.path.relpath("{}/data/cleaned/snapshot/{}_{}.csv".format(dirname(dirname(dirname(__file__))),'europe',str(date)))
europe.to_csv(path,index=True)


