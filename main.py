import csv
import boto3
import time
import datetime
from botocore.config import Config

from case import Case

cases_list = []
INTERVAL = 1  # Seconds


def read_csv(file_path):
    with open(file_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                case = Case(row[1], row[4], row[5], row[6],
                            convert_datetime_to_timestamp(row[7]), row[10])
                line_count += 1
                cases_list.append(case)


def write_records(records):
    try:
        result = write_client.write_records(DatabaseName="covid-timeseries",
                                            TableName="covid-timeseries-table",
                                            Records=records,
                                            CommonAttributes={})
        status = result['ResponseMetadata']['HTTPStatusCode']
        print("Processed %d records. WriteRecords Status: %s" %
              (len(records), status))
    except Exception as err:
        print("Error:", err)


def is_blank(value):
    if value and value.strip():
        return False
    return True


def convert_datetime_to_timestamp(current_time):
    timestamp = time.mktime(time.strptime(current_time, "%Y-%m-%d %H:%M:%S"))
    return int(timestamp * 1000)


def prepare_record(measure_name, measure_value, update_time, dimensions):
    if is_blank(measure_value):
        measure_value = 0.0
    record = {
        'Time': str(update_time),
        'Dimensions': dimensions,
        'MeasureName': measure_name,
        'MeasureValue': str(float(measure_value)),
        'MeasureValueType': 'DOUBLE'
    }
    return record


def start_data_ingestion():
    records = []
    while True:
        for record in cases_list:
            country = record.country
            confirmed_cases = record.confirmed_cases
            deaths = record.deaths
            recovered = record.recovered
            update_time = record.update_time
            region = record.region

            dimensions = [
                {'Name': 'country', 'Value': country},
                {'Name': 'region', 'Value': region}
            ]
            records.append(prepare_record('confirmed_cases', confirmed_cases, update_time, dimensions))
            records.append(prepare_record('deaths', deaths, update_time, dimensions))
            records.append(prepare_record('recovered', recovered, update_time, dimensions))

            print("records {} - country {} - confirmed_cases {} - deaths {} - recovered {}".format(
                len(records), country, confirmed_cases,
                deaths, recovered))

            if len(records) == 99:
                print("sending write request")
                write_records(records)
                records = []
            time.sleep(INTERVAL)


if __name__ == '__main__':
    read_csv('/Users/szafar/PycharmProjects/timeseries-covid/COVID-19_geo_timeseries.csv')

    session = boto3.Session(profile_name="playground")
    write_client = session.client('timestream-write', config=Config(
        read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))
    query_client = session.client('timestream-query')

    start_data_ingestion()
