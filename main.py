import csv

if __name__ == '__main__':
    with open('/Users/szafar/PycharmProjects/timeseries-covid/COVID-19_geo_timeseries.csv', newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                print(f'\t {row[1]} {row[4]} {row[5]} {row[6]} {row[7]} {row[10]}')
                line_count += 1
        print(f'Processed {line_count} lines.')