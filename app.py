from flask import Flask, jsonify
from spark_queries import *

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# Home route
@app.route("/")
def twitter_producer():
    return "This is Spark Assignment"

# Query - 1
@app.route("/max_diff_stock_daily_basis", methods=["GET"])
def max_diff_stock_daily_basis_api():
    result = max_diff_stock_daily_basis()
    dict_to_return = {"Max diff stock daily basis": result}
    return jsonify(dict_to_return)


# Query - 2
@app.route("/most_traded_stock_on_each_day", methods=["GET"])
def most_traded_stock_on_each_day_api():
    result = most_traded_stock_on_each_day()
    dict_to_return = {"Most traded stock on each day": result}
    return jsonify(dict_to_return)

# Query - 3
@app.route("/max_gap_up_and_gap_down", methods=["GET"])
def max_gap_up_and_gap_down_api():
    result = max_gap_up_and_gap_down()
    dict_to_return = {"Max gap up and gap down": result}
    return jsonify(dict_to_return)


# Query - 4
@app.route("/max_moved_stock", methods=["GET"])
def max_moved_stock_api():
    result = max_moved_stock()
    dict_to_return = {"Maximum Moved stock": result}
    return jsonify(dict_to_return)

# Query - 5
@app.route("/standard_deviation_for_stocks", methods=["GET"])
def standard_deviation_for_stocks_api():
    result = standard_deviation_for_stocks()
    dict_to_return = {"Standard Deviation": result}
    return jsonify(dict_to_return)

# Query - 6
@app.route("/mean_and_median_prices_for_stocks", methods=["GET"])
def mean_and_median_prices_for_stocks_api():
    result = mean_and_median_prices_for_stocks()
    dict_to_return = {"Mean and Median prices for stocks": result}
    return jsonify(dict_to_return)


# Query - 7
@app.route("/average_volume_for_stocks", methods=["GET"])
def average_volume_for_stocks_api():
    result = average_volume_for_stocks()
    dict_to_return = {"Average Volume for stocks": result}
    return jsonify(dict_to_return)

# Query - 8
@app.route("/highest_stock_average_volume", methods=["GET"])
def highest_stock_average_volume_api():
    result = highest_stock_average_volume()
    dict_to_return = {"Max Avg Volume stock": result}
    return jsonify(dict_to_return)

# Query - 9
@app.route("/highest_and_lowest_stock_prices", methods=["GET"])
def highest_and_lowest_stock_prices_api():
    result = highest_and_lowest_stock_prices()
    dict_to_return = {"Highest And lowest Value of stocks": result}
    return jsonify(dict_to_return)

if __name__ == '__main__':
    app.run(debug=True)