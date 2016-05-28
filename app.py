import argparse
import datetime
import logging

import data_processing as dp

if __name__ == '__main__':
    logging.basicConfig(filename='app.log', level=logging.INFO, filemode='w')
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('symbol', help='Символ финансового инструмента')
    parser.add_argument('-y', '--year', help='Год для анализа')
    parser.add_argument('-p', '--print', help='Вывести все данные', action='store_true')
    args = parser.parse_args()

    # Определяем год для чтения данных
    # Если год не указан берем текущий год
    if args.year:
        year = int(args.year)
    else:
        year = datetime.datetime.now().year
    logging.debug('Год {:d}'.format(year))
    start_date = datetime.date(year, 1, 1)
    end_date = datetime.date(year, 12, 31)

    logging.debug('Загрузка данных для ' + args.symbol)
    data = dp.google(args.symbol, start_date, end_date)

    if args.print:
        dp.print_data(data, title='Исходные данные')