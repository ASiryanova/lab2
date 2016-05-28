import datetime
import csv
from urllib.request import urlopen
from urllib.parse import quote_plus

def day_up_value(close_prices, max_value):
    day = 0
    for close_price in close_prices:
        if close_price < max_value:
            day = day + 1
    return day

def print_file(file_out, rezult):
    if file_out:
        with open(file_out, 'w') as f_out:
            f_out.write(str(rezult))
    print(rezult)

def process_network(symbol, up_value, year, file_out, file_log, level_log):
    start = datetime.date(year, 1, 1)
    end = datetime.date(year, 12, 31)
    url = "http://www.google.com/finance/historical?q={0}&startdate={1}&enddate={2}&output=csv"
    url = url.format(symbol.upper(), quote_plus(start.strftime('%b %d, %Y')), quote_plus(end.strftime('%b %d, %Y')))
    data = urlopen(url).readlines()
    close_prices = []
    for row in data[1:]:
        x=float(row.decode().strip().split(',')[4])
        close_prices.append(x)
    day = day_up_value(close_prices, up_value)
    print_file(file_out, day)

def process_file(file, up_value, file_out, file_log, level_log):
    with open(file) as f:
        f.readline()
        csv_file = csv.reader(f, delimiter=',')
        close_prices = []
        for row in csv_file:
            x=float(row[4])
            close_prices.append(x)
    day = day_up_value(close_prices, up_value)
    print_file(file_out, day)