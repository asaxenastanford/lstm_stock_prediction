from pandas_datareader import data
import matplotlib.pyplot as plt
import pandas as pd
import datetime as dt
import urllib.request, json 
import os
import numpy as np
import glob
import re
import sys

# Loading and preprocessing data
company_name = 'spy'
try:
	df = pd.read_csv(os.path.join('ETFs','spy.us.txt'),delimiter=',',usecols=['Date','Open','High','Low','Close'])
	df = df.sort_values('Date')
	df['Mid'] = df.apply(lambda row: (row.High + row.Low)/2.0, axis = 1) 
	dates = df['Date'].tolist()
except:
	print('Stock does not exist')
	sys.exit(1)

print(df)

# Processing Stock Data

# Dictionary for turning stock price time series data into supervised learning problem  (key: company, value: mid point price of that day)
all_mid_points = {} 
all_mid_points['Date'] = dates
all_mid_points[company_name] = df['Mid'].tolist()
all_stock_dfs = {}

all_stock_paths = glob.glob("/Users/aakankshasaxena/Documents/Junior/CS_230/Project/RNN_intro/stock_reduced/*.txt")
names = []
for path in all_stock_paths:
	name = path[79:]
	name = name.rstrip('.us.txt')	

	try:
		df_stock = pd.read_csv(path,delimiter=',',usecols=['Date','Open','High','Low','Close'])
		df_stock = df_stock.sort_values('Date')
		# add mid price data 
		df_stock['Mid'] = df_stock.apply(lambda row: (row.High + row.Low)/2.0, axis = 1) 
		# remove data for days that are missing from the original company 
		df_stock = df_stock[df_stock['Date'].isin(dates)]
		present_dates = df_stock['Date'].tolist()
		# add NaN (-1) data for days that are present in the main stock but not this stock 
		missing_dates = np.setdiff1d(dates,present_dates,True).tolist()
		missing_values = [-1] * len(missing_dates)
		missing_dates_data = list(zip(missing_dates, missing_values, missing_values, missing_values, missing_values, missing_values))
		missing_dates_df = pd.DataFrame(missing_dates_data, columns =['Date','Open','High','Low','Close','Mid Price'])
		df_stock = df_stock.append(missing_dates_df)
		df_stock = df_stock.sort_values('Date')
		df_stock.reset_index(drop=True, inplace=True)

		# add the data for this stock to the dictionary 
		all_stock_dfs[name] = df_stock
		names.append(name)
		# add midpoint data to all_mid_points dictionary 
		all_mid_points[name] = df_stock['Mid'].tolist()
	except:
		pass

# Final DF with mid point prices for every asset on every relevant date, along with a
# output value of the original asset shifted by 1 day
all_mid_points_df = pd.DataFrame(all_mid_points)
all_mid_points_df[company_name + ' (t+1)'] = all_mid_points_df[company_name].shift(-1)
print(all_mid_points_df)

# Processing Headline Data

all_headlines = pd.read_csv('abcnews-date-text.csv',delimiter=',',usecols=['publish_date','headline_text'])

# Reformat dates in all_headlines df to match the formatting of stocks 
headline_dates_orig = all_headlines['publish_date'].tolist()
headline_dates_mod = []
for orig_date in headline_dates_orig:
	orig_date = str(orig_date)
	year = orig_date[0:4]
	month = orig_date[4:6]
	date = orig_date[6:]
	final_date = year + "-" + month + '-' + date
	headline_dates_mod.append(final_date)

# Delete headlines from days that are not present in stock data, add NA neadlines for days missing but present in stock data
all_headlines['publish_date'] = headline_dates_mod
all_headlines = all_headlines.sort_values('publish_date')
all_headlines = all_headlines[all_headlines['publish_date'].isin(dates)]
present_dates = all_headlines['publish_date'].tolist()
missing_dates = np.setdiff1d(dates,present_dates,True).tolist()
missing_values = ['NA'] * len(missing_dates)
missing_dates_data = list(zip(missing_dates, missing_values))
missing_dates_df = pd.DataFrame(missing_dates_data, columns =['publish_date','headline_text'])
all_headlines = all_headlines.append(missing_dates_df)
all_headlines = all_headlines.sort_values('publish_date')
all_headlines.reset_index(drop=True, inplace=True)

# Consolidate all headlines from a given day into a list in one row in the DF
headline_dictionary = {} # collections.defaultdict()
for index, rows in all_headlines.iterrows(): 
	if rows.publish_date in headline_dictionary:
		headline_dictionary[rows.publish_date].append(rows.headline_text)
	else:
		headline_dictionary[rows.publish_date] = [rows.headline_text]

chronological_headlines = pd.DataFrame(columns=['Date','Headlines'])
for key, value in headline_dictionary:
	chronological_headlines.append({'Date': key, 'Headlines': value},ignore_index=True)

print('Loaded and sorted data')