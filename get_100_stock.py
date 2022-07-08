import json
import requests
import pandas as pd

key = 'e0c2a6b3b7mshb62f1170e459115p1acb7cjsn15bdddda8462'

def get_100_stocks():
	try:
		url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"

		headers = {
			"X-RapidAPI-Key": key,
			"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
		}
		response = requests.request("GET", url, headers=headers)
		res = json.loads(response.text)
		stocks = res['stocks']
		stocks = stocks[:100]
		return stocks
	except Exception as e:
		print(e)

def get_historical_data():
	try:
		stocks = get_100_stocks()
		for i in range(len(stocks)):
			url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
			querystring = {"ticker_symbol": str(stocks[i]), "years": "1", "format": "json"}
			headers = {
				"X-RapidAPI-Key": key,
				"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
			}
			response = requests.request("GET", url, headers=headers, params=querystring)
			res = json.loads(response.text)
			df = pd.DataFrame(res['historical prices'])
			df['Company'] = stocks[i]
			df.to_csv('csv_database/'+str(stocks[i]+'.csv'))
	except Exception as e:
		print(e)




