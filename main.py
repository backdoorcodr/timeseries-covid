import csv
import boto3
from botocore.config import Config

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

def write_records(records):
    session = boto3.Session()
    write_client = session.client('timestream-write', config=Config(
        read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))
    try:
        result = write_client.write_records(DatabaseName="DATABASE_NAME",
                                            TableName="TABLE_NAME",
                                            Records=records,
                                            CommonAttributes={})
        status = result['ResponseMetadata']['HTTPStatusCode']
        print("Processed %d records. WriteRecords Status: %s" %
              (len(records), status))
    except Exception as err:
        print("Error:", err)

def prepare_record(measure_name, measure_value, time, dimensions):
    record = {
        'Time': str(time),
        'Dimensions': dimensions,
        'MeasureName': measure_name,
        'MeasureValue': str(measure_value),
        'MeasureValueType': 'DOUBLE'
    }
    return record

def prepare_record():

    records = []

    for record in cases_list:
        country = record.country
        confirmed_cases = record.confirmed_cases
        deaths = record.deaths
        recovered = record.recovered
        update_time = record.recovered
        region = record.region

        dimensions = [
            {'Name': 'region', 'Value': region}
        ]

        records.append(prepare_record('country', country, update_time, dimensions))
        records.append(prepare_record('confirmed_cases', confirmed_cases, update_time, dimensions))
        records.append(prepare_record('deaths', deaths, update_time, dimensions))
        records.append(prepare_record('recovered', recovered, update_time, dimensions))

        print("records {} - country {} - confirmed_cases {} - deaths {} - recovered {}".format(
            len(records), country, confirmed_cases,
            deaths, recovered))



if __name__ == '__main__':
    read_csv('/Users/szafar/PycharmProjects/timeseries-covid/COVID-19_geo_timeseries.csv')

    session = boto3.Session(profile_name="playground")
    write_client = session.client('timestream-write', config=Config(
        read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))
    query_client = session.client('timestream-query')

    while True:
        prepare_record()

