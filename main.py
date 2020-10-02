import csv

from case import Case

cases_list = []
def read_csv(file_path):
    with open(file_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                case = Case(row[1], row[4], row[5], row[6], row[7], row[10])
                line_count += 1
                cases_list.append(case)
    print(f'Processed {len(cases_list)} lines.')


if __name__ == '__main__':
    read_csv('/Users/szafar/PycharmProjects/timeseries-covid/COVID-19_geo_timeseries.csv')


