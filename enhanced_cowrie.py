"""
This script takes a cowrie "JSON" file and blends it with IP location data and outputs the results to CSV / PostgreSQL.
"""

import argparse
import csv
import json
import multiprocessing as mp
import operator
import os
import pandas as pd
import socket
import struct
import sys
from sqlalchemy import create_engine
from tqdm import tqdm


def combine_dicts(first_dict, second_dict, op=operator.add):
    return dict(first_dict.items() + second_dict.items() + [(keys, op(first_dict[keys], second_dict[keys])) for keys in
                                                            set(second_dict) & set(first_dict)])


def do_exit(item=None, file_format=None):
    """
    Outputs the usage information and exits.
    :param item: the item that failed validation.
    :param file_format: the format the file is supposed to be.
    """
    print ""
    print "Usage: python cowrie_to_csv.py <filename> [sql] [ip location]"
    print "    <filename> should be a cowrie file of serialised JSON documents that is to be converted."
    if not item == 'none':
        print "    {} does not appear to be a valid {} file.".format(item, file_format.upper())
    print ""

    sys.exit(1)


def insert_into_postgres(file_name):
    """
    Takes the csv output and inserts it into a Postgresql database
    :param file_name:
    :return:
    """
    username = 'postgres'
    password = 'password'
    sql_server = '127.0.0.1'
    db_name = 'cowrie'
    table_name = 'cowrie_enriched_data'

    engine = create_engine('postgresql://{}:{}@{}/{}'.format(username, password, sql_server, db_name))
    df = pd.read_csv(file_name)
    print "Inserting table [{}] into database [{}] as user [{}] ...".format(table_name, db_name, username)
    df.to_sql(table_name, engine, if_exists='append')


def is_ip_address(test_value_is_ip):
    """
    Returns True if test_value is an IP address otherwise False.
    :param test_value_is_ip:
    :return: boolean:
    """
    try:
        socket.inet_aton(test_value_is_ip)
        return True
    except:
        return False


def parse_record(log_record):
    line_match_loc = {}
    new_log_record = {}
    for k in log_record:
        if is_ip_address(log_record[k]):
            k_code = k + '_code'
            k_country = k + '_country'
            k_region = k + '_region'
            k_city = k + '_city'
            ip_to_dec = int(struct.unpack('!I', socket.inet_aton(log_record[k]))[0])
            df_match = IP_DATA[(IP_DATA['start'] <= ip_to_dec) & (IP_DATA['end'] >= ip_to_dec)]
            line_match_loc[k_code] = df_match.iloc[0]['code']
            line_match_loc[k_country] = df_match.iloc[0]['country']
            line_match_loc[k_region] = df_match.iloc[0]['region']
            line_match_loc[k_city] = df_match.iloc[0]['city']
        new_log_record = combine_dicts(log_record, line_match_loc)
    return new_log_record


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('input_file')
    parser.add_argument('output_sql', nargs='?', default='csv')
    parser.add_argument('ip_location', nargs='?', default='IP2LOCATION-LITE-DB9.CSV')

    args = parser.parse_args()
    if args.input_file is None:
        do_exit()
    else:
        input_file = args.input_file
        if args.output_sql and args.output_sql.lower() == 'sql':
            output_sql = True
        else:
            output_sql = False

        ip_location_file = os.path.join(os.path.dirname(__file__), 'IP2LOCATION-LITE-DB9.CSV')
        if args.ip_location and os.path.isfile(args.ip_location):
            ip_location_file = args.ip_location
        elif not os.path.isfile(ip_location_file):
            do_exit(ip_location_file, 'csv')

        # Correct JSON file
        corrected_file = os.path.splitext(input_file)[0] + '.corrected.json'
        print "Fixing {} by creating {} so it's really JSON.".format(input_file, corrected_file)
        try:
            with open(input_file, 'r') as original:
                original_data = original.read()
                with open(corrected_file, 'w') as corrected:
                    corrected.write(
                        '{\"json_data\":[' + original_data.replace('}{', '},{').replace('}\n{', '},{') + ']}')
        except IOError:
            do_exit(input_file)

        # Enrich JSON file
        enhanced_file = os.path.splitext(input_file)[0] + '.enhanced.json'
        print "Enriching {} by adding data from {} ...".format(corrected_file, ip_location_file)
        results = []
        try:
            IP_DATA = pd.read_csv(ip_location_file, header=None, delimiter=',', encoding='utf-8-sig', quotechar='"',
                                  names=['start', 'end', 'code', 'country', 'region', 'city', 'lat', 'long', 'pc'],
                                  usecols=['start', 'end', 'code', 'country', 'region', 'city'])
            with open(corrected_file, 'r') as corrected:
                corrected_data = json.load(corrected)['json_data']
                mp_p_number = mp.cpu_count()
                mp_pool = mp.Pool(processes=mp_p_number)
                print "  splitting work into {} processes ...".format(mp_p_number)
                for result in tqdm(mp_pool.imap_unordered(parse_record, corrected_data), total=len(corrected_data)):
                    results.append(result)
                new_enhanced = {'json_data': [results]}
        except IOError:
            do_exit(ip_location_file, 'csv')

        # Output CSV file
        output_file = os.path.splitext(input_file)[0] + '.csv'
        print "Writing {} from {} as real CSV.".format(output_file, enhanced_file)
        json_data_keys = set()
        with open(output_file, 'w') as csv_file:
            csv_data = csv.writer(csv_file)
            cnt = 0
            for csv_record in results:
                for csv_k in csv_record:
                    json_data_keys.add(csv_k)
            csv_data.writerow(list(json_data_keys))
            for csv_record in tqdm(results):
                json_data_values = []
                for csv_k in json_data_keys:
                    try:
                        json_data_values.append(csv_record[csv_k])
                    except KeyError:
                        json_data_values.append('')
                csv_data.writerow(json_data_values)

        if output_sql:
            insert_into_postgres(output_file)
            os.remove(output_file)

        if os.path.exists(corrected_file):
            os.remove(corrected_file)
            if os.path.exists(enhanced_file):
                os.remove(enhanced_file)
            exit()
