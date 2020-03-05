import pandas as pd
import os, requests, bs4
from os.path import dirname as dirname
from ftplib import FTP
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class TickerLoader:

	def __init__(self):
		#self.path_nyse = os.path.relpath("{}/data/raw/tickers/{}.csv".format(dirname(dirname(dirname(__file__))),'nyse_listed_2018'))
		#self.path_sec = os.path.relpath("{}/data/raw/tickers/{}.csv".format(dirname(dirname(dirname(__file__))),'cik_ticker'))

		self.nasdaq_path = os.path.relpath("{}/data/raw/tickers/{}.txt".format(dirname(dirname(dirname(__file__))),'nasdaq'))
		self.other_path = os.path.relpath("{}/data/raw/tickers/{}.txt".format(dirname(dirname(dirname(__file__))),'other_sec'))

		self.path_lse = os.path.relpath("{}/data/raw/tickers/{}.csv".format(dirname(dirname(dirname(__file__))),'lse'))
		self.path_euronext = os.path.relpath("{}/data/raw/tickers/{}.csv".format(dirname(dirname(dirname(__file__))),'euronext_2019'))
		self.path_frankfurt = os.path.relpath("{}/data/raw/tickers/{}.csv".format(dirname(dirname(dirname(__file__))),'deutsche_borse'))

	def getTickers(self,name,filterTickers=True,exchangeSuffix=False):

		if name == 'sec':
			#tickers = pd.read_csv(self.path_sec,delimiter='|')[['CIK','Ticker']].dropna()
			#tickers = list(tickers['Ticker'])
			nasdaq_tickers = pd.read_csv(self.nasdaq_path,delimiter='|')['Symbol'].dropna().tolist()
			other_tickers = pd.read_csv(self.other_path,delimiter='|')['ACT Symbol'].dropna().tolist()
			tickers = list(set(nasdaq_tickers + other_tickers))
			tickers.sort()
		#elif name == 'nyse':
		#	tickers = list(pd.read_csv(self.path_nyse)['ACT Symbol'])
		elif name == 'lse':
			tickers = list(pd.read_csv(self.path_lse)['Symbol'])
			if exchangeSuffix:
				tickers = [ticker+'.L' for ticker in tickers]
		elif name == 'euronext':

			def exchangeSymbol(market):
				if 'Paris' in market:
					return 'PA'
				elif 'Amsterdam' in market:
					return 'AS'
				elif 'Brussels' in market:
					return 'BR'
				elif 'Dublin' in market:
					return 'IR'
				elif 'Lisbon' in market:
					return 'LS'

			data = pd.read_csv(self.path_euronext,delimiter=';')
			if exchangeSuffix:
				data['Symbol'] = data.apply(lambda row: row.Symbol+"."+exchangeSymbol(row.Market),axis=1)
			tickers = list(data['Symbol'])
		elif name == 'frankfurt':
			tickers = list(pd.read_csv(self.path_frankfurt,names=['Symbol',])['Symbol'])
			if exchangeSuffix:
				tickers = [ticker+'.F' for ticker in tickers]

		if filterTickers:
			return list(filter(lambda x: not x.strip('ABCDEFGHIJKLMNOPQRSTUVWXYZ'),tickers))
		else:
			return tickers

	def scrapeTickers(self,name):
		if name == 'lse':
			url = "https://www.londonstockexchange.com/exchange/prices-and-markets/international-markets/markets/securities/XLON.html?&page={}"
			response = requests.get(url.format(1)).text
			pageCountTag = '<p class="floatsx">&nbsp;Page 1 of '
			pageCountEndTag = '</p>'
			pageCountIndex = response.find(pageCountTag) + len(pageCountTag)
			response = response[pageCountIndex:]
			pageCountEndIndex = response.find(pageCountEndTag)
			pageCount = int(response[:pageCountEndIndex])

			result = pd.DataFrame()
			for pageNb in range(1,pageCount+1):
				response = requests.get(url.format(pageNb))
				soup = bs4.BeautifulSoup(response.content, 'html.parser')
				table = str(soup.find_all('table')[0])
				table = pd.read_html(table)[0]
				result = pd.concat((result,table),axis=0)

			result['Symbol'] = result.Symbol.apply(lambda ticker: ticker[:-1])
			result.to_csv(self.path_lse,index=False)

		elif name == 'sec':
			ftp = FTP("ftp.nasdaqtrader.com", "anonymous", "")
			ftp.login()
			ftp.cwd("SymbolDirectory")



			with open(self.nasdaq_path, "wb") as lf:
				ftp.retrbinary("RETR " + 'nasdaqlisted.txt', lf.write)

			with open(self.other_path, "wb") as lf:
				ftp.retrbinary("RETR " + 'otherlisted.txt', lf.write)

		elif name == 'euronext':
			# request = "https://live.euronext.com/pd/data/stocks/download?mics=ALXB%2CALXL%2CALXP%2CXPAR%2CXAMS%2CXBRU%2CXLIS%2CXMLI%2CMLXB%2CENXB%2CENXL%2CTNLA%2CTNLB%2CXLDN%2CXESM%2CXMSM%2CXATL%2CVPXB&display_datapoints=dp_stocks&display_filters=df_stocks"
			#
			# chrome_options = Options()
			# chrome_options.add_argument("--headless")
			# chrome_options.add_argument("--window-size=1024x1400")
			# chrome_options.add_argument("--chromever=80.0.3987.16")
			# chrome_options.add_experimental_option("prefs", {
			# 		"download.default_directory": os.path.relpath("{}/bin/".format(dirname(dirname(dirname(__file__))))),
			# 		"download.prompt_for_download": False,
			# 		"download.directory_upgrade": True,
			# 		"safebrowsing_for_trusted_sources_enabled": False,
			# 		"safebrowsing.enabled": False
			# })
			#
			# def enable_download_headless(browser,download_dir):
			# 	browser.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
			# 	params = {'cmd':'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
			# 	browser.execute("send_command", params)
			#
			# chrome_driver = os.path.relpath("{}/bin/chromedriver".format(dirname(dirname(dirname(__file__)))))
			# driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chrome_driver)
			#
			# driver.get("https://live.euronext.com/en/products/equities/list")
			# download_button = WebDriverWait(driver,1).until(EC.presence_of_element_located((By.XPATH,'//*[@id="stocks-data-table_wrapper"]/div[1]/div[2]/button[2]')))
			# download_button.click()
			# WebDriverWait(driver,1).until(EC.visibility_of_element_located((By.XPATH,'//*[@id="downloadModal"]/div/div/div[2]/fieldset[1]')))
			# csv_option = WebDriverWait(driver,1).until(EC.presence_of_element_located((By.XPATH,'//*[@id="downloadModal"]/div/div/div[2]/fieldset[1]/div[1]/label')))
			# csv_option.click()
			# submit = WebDriverWait(driver,1).until(EC.presence_of_element_located((By.XPATH,'//*[@id="downloadModal"]/div/div/div[2]/input')))
			# submit.click()
			# driver.get_screenshot_as_file(self.path_euronext+".png")
			pass

		elif name == 'frankfurt':
			pass

