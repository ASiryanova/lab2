import sys
import csv

def day_up_value(close_prices, max_value):
    day = 0
    for close_price in close_prices:
        if close_price <= max_value: 
            day = day + 1
    return day
    

def main(file_name, max_value, file_name_print=None):
    with open(file_name) as f:
        csv_file = csv.reader(f, delimiter=',')
        close_prices = []
        for row in csv_file:
            x=float((row[4]))
            close_prices.append(x)
    day = day_up_value(close_prices, max_value)        
    if file_name_print:
        with open(file_name_print, 'w') as f_out:
            f_out.write(str(day))

        #f_out = open(file_name_print, 'w')
        #f_out.write(str(day))
    print(day)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        if len(sys.argv) > 2:
            max_value = sys.argv[2]
            file_name_print = sys.argv[3] if len(sys.argv) > 3 else None
            main(file_name, float(max_value), file_name_print)
        else:
            print("Укажите уровень закрытия цены")
    else:    
        print("Укажите имя файла")



