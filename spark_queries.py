from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Stock Project').getOrCreate()
# read all the csv files in the directory into a spark dataframe
spark_df = spark.read.csv('csv_database/*.csv', sep=',', header=True)
# converting the Date column from string format to date format
spark_df = spark_df.withColumn('Date', spark_df['Date'].cast('date'))
# creating a temporary view i.e, a table of a dataframe
spark_df.createTempView('stocks')

def max_diff_stock_daily_basis():
    try:
        query1 = """Select stock_table.company, stock_table.date, stock_table.max_diff_stock_percent from 
                 (Select date,company,((high-open)/open)*100 as max_diff_stock_percent, dense_rank()
                 OVER ( partition by date order by ( high-open)/open desc ) as dense_rank FROM stocks)stock_table 
                 where stock_table.dense_rank=1"""

        # storing data into a variable
        data = spark.sql(query1).collect()
        results = {}
        for row in data:
            results[row['date'].strftime('%Y-%m-%d')] = {'company': row['company'],
                                                         'max_diff_stock_percent': row['max_diff_stock_percent']}
        return results
    except Exception as e:
        return {'Error' : e}

def most_traded_stock_on_each_day():
    try:
        query = """
        Select stock_table.company, stock_table.date, stock_table.volume from (Select date, company, volume,
        dense_rank() over (partition by date order by int(volume) desc) as dense_rank from stocks)stock_table
        where stock_table.dense_rank=1
        """
        # storing data into a variable
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results[row['date'].strftime("%Y-%m-%d")] = {'company': row['company'], 'date': row['date'], 'volume': row['volume']}
        return results
    except Exception as e:
        return {'Error' : e}

def max_gap_up_and_gap_down():
    try:
        query = """
                Select stocks_table.company,abs(stocks_table.previous_close-stocks_table.open) as max_gap from 
                (Select company, open, date, close, lag(close,1,35.724998) over(partition by company order by date) as
                previous_close from stocks asc)stocks_table order by max_gap desc limit 1
            """
        # storing data into a variable
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results['company'] = row['company']
            results['max_gap'] = row['max_gap']
        return results
    except Exception as e:
        return {'Error' : e}

def max_moved_stock():
    try:
        query = """
        with df1 as (select company, open from (select company, open, dense_rank() over (partition by company order by date) as d_rank1 from stocks)stock_table where stock_table.d_rank1=1)
          , df2 as (select company, close from (select company, close, dense_rank() over (partition by company order by date desc) as d_rank2 from stocks)stock_table2 where stock_table2.d_rank2 = 1)
          select df1.company, df1.open, df2.close, df1.open-df2.close as max_diff from df1 inner join df2 where df1.company = df2.company
          order by max_diff DESC limit 1
        """
        # storing data into a variable
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results['company'] = row['company']
            results['open'] = row['open']
            results['close'] = row['close']
            results['max_diff'] = row['max_diff']
        return results
    except Exception as e:
        return {'Error': e}


def standard_deviation_for_stocks():
    try:
        query = """
            select Company, stddev_samp(Volume) as Standard_Deviation from stocks group by Company
        """
        # storing data into a variable
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'Company': key, 'Standard_Deviation': val})
        return results
    except Exception as e:
        return {'Error' : e}

def mean_and_median_prices_for_stocks():
    try:
        query = """
                Select company, avg(Close) as mean, percentile_approx(Close,0.5) as median from stocks group by company
            """
        # storing data into a variable
        data = spark.sql(query).collect()
        results = []
        for row in data:
            results.append({'company': row['company'], 'mean': row['mean'], 'median': row['median']})
        return results
    except Exception as e:
        return {'Error' : e}

def average_volume_for_stocks():
    try:
        query = """
            select Company, AVG(Volume) as Average_Volume from stocks group by Company order by Average_Volume desc
        """
        # storing data into a variable
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'Company': key, 'Average Volume': val})
        return results
    except Exception as e:
        return {'Error': e}


def highest_stock_average_volume():
    try:
        query = """
            select Company, AVG(Volume) as Average_Volume from stocks group by Company order by Average_Volume desc limit 1
        """
        spark.sql(query).show()
        # storing data into a variable
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'Company': key, 'Average Volume': val})
        return results
    except Exception as e:
        return {'Error' : e}

def highest_and_lowest_stock_prices():
    try:
        query = """
            select Company, MAX(high) as Highest_Price, MIN(low) as Lowest_Price from stocks group by Company
        """
        # storing data into a variable
        data = spark.sql(query).collect()
        results = []
        for row in data:
            results.append(
                {'company': row['Company'], 'highest_price': row['Highest_Price'], 'lowest_price': row['Lowest_Price']})
        return results
    except Exception as e:
        return {'Error' : e}
