from urllib.request import urlopen
from urllib.parse import quote_plus
import datetime

GOOGLE_FINANCE_URL = "http://www.google.com/finance/historical?" \
                     "q={0}&startdate={1}&enddate={2}&output=csv"


def google(symbol, start, end):
    """
    Метод читает данные с сервиса Google Finance
    Формат: Date,Open,High,Low,Close,Volume
    :param symbol: символ
    :param start: дата начала
    :param end: дата конца
    :return: словарь с полученными данными
    """
    url = GOOGLE_FINANCE_URL.format(
        symbol.upper(),
        quote_plus(start.strftime('%b %d, %Y')),
        quote_plus(end.strftime('%b %d, %Y'))
    )
    # Делаем словарь для хранения наших данных в виде одного объекта
    # Для хранения таких данных лучше подходят Pandas Data Frame
    # http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html
    data = {
        'date': [],
        'open': [],
        'close': [],
        'high': [],
        'low': [],
        'volume': [],
        'table': {},  # Для хранения данных по строкам
        # Мета информация о наших колонках, нужна только для печати
        'columns': ['open', 'close', 'high', 'low', 'volume']
    }
    raw_data = urlopen(url).readlines()
    for line in raw_data[1:]:  # Пропускаем первую строку с именами колонок
        row = line.decode().strip().split(',')
        date = datetime.datetime.strptime(row[0], '%d-%b-%y').date()
        open_price = float(row[1])
        close_price = float(row[2])
        high_price = float(row[3])
        low_price = float(row[4])
        volume_price = float(row[5])
        data['date'].append(date)
        data['open'].append(open_price)
        data['close'].append(close_price)
        data['high'].append(high_price)
        data['low'].append(low_price)
        data['volume'].append(volume_price)
        data['table'][date] = {
            'open': open_price,
            'close': close_price,
            'high': high_price,
            'low': low_price,
            'volume': volume_price,
        }
    return data

def print_data(data, width=15, title=''):
    # Форматированный вывод исходнных данных, если пользователь указал флаг
    columns_count = 1
    head = 'DATE'.center(width)

    row_format = '{date!s:^{width}}'
    if 'columns' in data:
        columns_count += len(data['columns'])
        for column in data['columns']:
            head += column.upper().center(width)
            row_format += '{' + column + ':^{width}.3f}'
    if 'text_columns' in data:
        columns_count += len(data['text_columns'])
        for column in data['text_columns']:
            head += column.upper().center(width)
            row_format += '{' + column + ':^{width}}'
    print(title.center(columns_count * width))
    print(head.center(columns_count * width))
    # Сортируем ключи и выводим
    for date in sorted(data['table'].keys()):
        print(row_format.format(date=date, **data['table'][date], width=width))