# BRIKS Screener
Stock screener created by Anthony Rathé for BRIKS

## About
This repository contains the source code of a custom developed stock screener, which has been deployed on Google Cloud. The stock screener is automatically executed on a daily basis and completes the following steps:
- fetch the latest up-to-date list of publicly traded stocks and their correpsponding ticker symbols, covering the following markets: all US markets, London Stock Exchange, Frankfurt Stock Exchange and Euronext (Paris, Lisbon, Brussels, Amsterdam)
- fetch the latest quarterly earnings reports for all US-traded companies (source: Securities and Exchange Commision)
- fetch the latest fundamental data (revenues, net earnings, EBITDA, liabilities, dividends, ...) for all European companies (source: Yahoo Finance)
- fetch the latest historical price & general (description, analyst ratings) data for all (US & European) companies (source: Yahoo Finance) 
- process & store the collected data
- consolidate the collected data into a Google Sheets-based stock screener: an outdated example can be found at https://docs.google.com/spreadsheets/d/1uG8e34sEZHlLE3g-QjFJuIiEAAo8fzZdT9btBhkx7qg/edit?usp=sharing 
- consolidate the collected data into a Powerpoint-based company summary booklet: an outdated example can be found at https://docs.google.com/presentation/d/1WynDt0QHHlkGrOmNg2EQlqllftnDeExh/edit?usp=sharing&ouid=117969324967018108673&rtpof=true&sd=true (please note that this example contains only a small subset of US-listed companies)


