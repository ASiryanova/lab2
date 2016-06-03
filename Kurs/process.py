import datetime
import csv
from urllib.request import urlopen
from urllib.parse import quote_plus
import logging

def day_up_value(info, max_value):
    day = 0
    for row_info in info:
        if row_info < max_value:
            day = day + 1
    return day

def day_CCI(data, window=10):
    try:
        import numpy as np
        Pt = (np.array(data['high']) + np.array(data['low']) + np.array(data['close']))/3
        weight = np.ones((window,)) / window
        sma = np.convolve(Pt, weight)[:len(np.array(data['high']))]
        mad = np.convolve(Pt-sma, weight)[:len(np.array(data['high']))]
        CCI = 1/0.0015*(np.array(Pt)-np.array(sma))/np.array(mad)
    except ImportError:
        logging.error('Библиотека Numpy не найдена')
    dates = data['date'][window:]
    CCI = list(CCI[window:])
    data = {
        'date': dates,
        'CCI': CCI,
        'table': {date: {'CCI': CCI[i]} for i, date in enumerate(dates)},
        'columns': ['CCI']
        }
    return data

def sma_signals(data, CCI):
    result = {}
    pred_CCI = None
    # дату отсортируем по возрастанию, чтобы можно было корректно обработать данные sorted
    for date in sorted(CCI['table'].keys()):
        now_CCI = CCI['table'][date]['CCI']
        if pred_CCI == None:
            pred_CCI = now_CCI
        if (pred_CCI < 0) and (now_CCI > 0):
            result[date] = {'signal': 'BUY'}
        elif (pred_CCI > 0) and (now_CCI < 0):
            result[date] = {'signal': 'SELL'}
        pred_CCI = now_CCI

    return {
        'table': result,
        'text_columns': ['signal']
    }

def print_file(file_out, rezult):
    if file_out:
        with open(file_out, 'w') as f_out:
            f_out.write(str(rezult))
    print(rezult)

def print_file_signal(file_out, data, width=15):
    row_format = '{date!s:^{width}}'
    if file_out:
        with open(file_out, 'w') as f_out:
            for date in sorted(data['table'].keys()):
                f_out.write(row_format.format(date=date, **data['table'][date], width=width) + data['table'][date]['signal'] + '\n')
                print(row_format.format(date=date, **data['table'][date], width=width) + data['table'][date]['signal'])

def read_url(symbol, year):
    start = datetime.date(year, 1, 1)
    end = datetime.date(year, 12, 31)
    url = "http://www.google.com/finance/historical?q={0}&startdate={1}&enddate={2}&output=csv"
    url = url.format(symbol.upper(), quote_plus(start.strftime('%b %d, %Y')), quote_plus(end.strftime('%b %d, %Y')))
    data = urlopen(url).readlines()

    info = {
        'date': [],
        'open': [],
        'close': [],
        'high': [],
        'low': [],
        'volume': [],
        'table': {},  # Для хранения данных по строкам
        #информация колонках
        'columns': ['open', 'close', 'high', 'low', 'volume']
    }

    for line in data[1:]:  # Пропускаем первую строку с именами колонок
        row = line.decode().strip().split(',')
        date = datetime.datetime.strptime(row[0], '%d-%b-%y').date()
        open_price = float(row[1])
        close_price = float(row[2])
        high_price = float(row[3])
        low_price = float(row[4])
        volume_price = float(row[5])
        info['date'].append(date)
        info['open'].append(open_price)
        info['close'].append(close_price)
        info['high'].append(high_price)
        info['low'].append(low_price)
        info['volume'].append(volume_price)
        info['table'][date] = {
            'open': open_price,
            'close': close_price,
            'high': high_price,
            'low': low_price,
            'volume': volume_price,
        }

    return info

def read_file(file):
    info = {
        'date': [],
        'open': [],
        'close': [],
        'high': [],
        'low': [],
        'volume': [],
        'table': {},  # Для хранения данных по строкам
        #информация колонках
        'columns': ['open', 'close', 'high', 'low', 'volume']
    }
    with open(file) as f:
        f.readline()
        csv_file = csv.reader(f, delimiter=',')
        close_prices = []
        for row in csv_file:
            date = row[0]
            open_price = float(row[1])
            close_price = float(row[4])
            high_price = float(row[2])
            low_price = float(row[3])
            volume_price = float(row[5])
            info['date'].append(date)
            info['open'].append(open_price)
            info['close'].append(close_price)
            info['high'].append(high_price)
            info['low'].append(low_price)
            info['volume'].append(volume_price)
            info['table'][date] = {
             'open': open_price,
             'close': close_price,
             'high': high_price,
             'low': low_price,
             'volume': volume_price,
             }
    return info

def process_data(data, up_value, file_out, indicator):
    if indicator is None:
        day = day_up_value(data["close"], up_value)
        print_file(file_out, day)
    if indicator == "CCI":
        CCI = day_CCI(data)
        signals = sma_signals(data, CCI)
        print_file_signal(file_out, signals)


def process_network(symbol, up_value, year, file_out, indicator):
    data = read_url(symbol, year)
    process_data(data, up_value, file_out, indicator)

def process_file(file, up_value, file_out, indicator):
    data = read_file(file)
    process_data(data, up_value, file_out, indicator)