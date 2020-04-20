"""Combines data from the 3 csv files generated from bcp data by import.py:
extract_charity.csv, extract_financial.csv, extract_partb.csv

Returns a csv per financial year, with a row per main charity.
"""
import csv
import sys
from collections import defaultdict

from constants import PRIMARY_REGNOS, FINANCIAL_YEARS, FILTERED_HEADINGS_TABLE_MAP, \
    ALL_FILTERED_HEADINGS, FINANCIAL_FILTERED_HEADINGS


def remove_null_bytes(file):
    for line in file:
        yield line.replace('\0', ' ')


def update_financial_data_dict(file, regno_index, fystart_index, data_dict):
    reader = csv.reader(file, delimiter=',')
    headers = next(reader)
    for row in reader:
        if not row[0] or len(row) < 2:
            continue
        regno = row[regno_index]
        fystart = row[fystart_index]
        fyear = fystart[:4]
        if fyear not in FINANCIAL_YEARS:
            continue
        data_dict[regno][fyear].update(
            {headers[index]: column for index, column in enumerate(row)}
        )
    return data_dict


def compile_to_npc_format(file_path, primary_regnos_only=False):
    financial_data_dict = defaultdict(lambda: defaultdict(dict))
    charities_data_dict = {}

    with open(file_path + 'extract_financial.csv', encoding='utf-8') as financial_file:
        financial_data_dict = update_financial_data_dict(financial_file, 0, 1, financial_data_dict)

    with open(file_path + 'extract_partb.csv', encoding='utf-8') as partb_file:
        financial_data_dict = update_financial_data_dict(partb_file, 0, 2, financial_data_dict)

    with open(file_path + 'extract_charity.csv', encoding='utf-8') as charity_file:
        charity_file = remove_null_bytes(charity_file)
        charity_reader = csv.reader(charity_file, delimiter=',')
        charity_headers = next(charity_reader)
        for row in charity_reader:
            if not row[0] or len(row) < 2:
                continue
            regno = row[0]
            subno = row[1]
            if subno != '0':
                continue
            charities_data_dict[regno] = {
                charity_headers[index]: column for
                index, column in
                enumerate(row[:len(charity_headers)])
            }

    for year in FINANCIAL_YEARS:
        file_suffix = 'primary' if primary_regnos_only else 'all'
        with open(
            file_path + 'npc_format_{year}_{suffix}.csv'.format(year=year, suffix=file_suffix),
            'w', newline='', encoding='utf-8'
        ) as ncp_format_file:
            ncp_writer = csv.writer(ncp_format_file, delimiter=',')
            all_headings = ALL_FILTERED_HEADINGS
            ncp_writer.writerow(all_headings)
            for regno, charities_data in charities_data_dict.items():
                # this check is slow, so do it here once rather than for
                # each file above
                if primary_regnos_only and regno not in PRIMARY_REGNOS:
                    continue
                charity_row = [
                    charities_data.get(header, '') for
                    header in FILTERED_HEADINGS_TABLE_MAP['extract_charity']
                ]
                financial_data = financial_data_dict.get(regno, {}).get(year, {})
                financial_row = [
                    financial_data.get(header, '') for
                    header in FINANCIAL_FILTERED_HEADINGS
                ]
                ncp_writer.writerow(charity_row + financial_row)


def main():
    if len(sys.argv) >= 2:
        file_path = sys.argv[1]
        primary_regnos_only = False
        if len(sys.argv) == 3:
            if sys.argv[2] == 'primary':
                primary_regnos_only = True
            else:
                print("ERROR: Filter argument %s is not recognised" % sys.argv[2])
                return
        compile_to_npc_format(file_path, primary_regnos_only=primary_regnos_only)
    else:
        print(
            "ERROR: No file path to folder containing extract_charity.csv"
            "extract_partb.csv and extract_financial.csv was provided.")


if __name__ == '__main__':
    main()
