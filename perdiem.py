import json
from bs4 import BeautifulSoup
from bs4 import element
import requests
import csv
from datetime import datetime
import re
import random
import time
import os


ZIP_MATCHER = re.compile(r'\d{5}($|(-\d{4}))')

RECORD_CATEGORIES = {
    'TWO_TABLE': 'TWO_TABLE'
}


RATES_2017 = 'rates_2017.json'
RATE_YEAR_PATHS = {
    '2018': 'rates_2018.json',
    '2017': 'rates_2017.json',
    '2016': 'rates_2016.json',
    '2015': 'rates_2015.json'
}
TABLE_YEAR_PATHS = {
    '2018': 'gsa_tables_2018.json',
    '2017': 'gsa_tables_2017.json',
    '2016': 'gsa_tables_2016.json',
    '2015': 'gsa_tables_2015.json'
}

RATE_DATE_FORMAT = '%Y-%m'

RATE_DATES_2018 = ['2017-10', '2017-11', '2017-12',
                   '2018-01', '2018-02', '2018-03',
                   '2018-04', '2018-05', '2018-06',
                   '2018-07', '2018-08', '2018-09']

RATE_DATES_2017 = ['2016-10', '2016-11', '2016-12',
                   '2017-01', '2017-02', '2017-03',
                   '2017-04', '2017-05', '2017-06',
                   '2017-07', '2017-08', '2017-09']


RATE_DATES_2016 = [
    '2015-10', '2015-11', '2015-12',
    '2016-01', '2016-02', '2016-03',
    '2016-04', '2016-05', '2016-06',
    '2016-07', '2016-08', '2016-09']


RATE_DATES_2015 = [
    '2014-10', '2014-11', '2014-12',
    '2015-01', '2015-02', '2015-03',
    '2015-04', '2015-05', '2015-06',
    '2015-07', '2015-08', '2015-09']

RATE_YEARS = {
    '2018': RATE_DATES_2018,
    '2017': RATE_DATES_2017,
    '2016': RATE_DATES_2016,
    '2015': RATE_DATES_2015
}

FISCAL_YEARS = {
    '2018': (datetime.strptime('10/01/2017', '%m/%d/%Y'), datetime.strptime('9/30/2018', '%m/%d/%Y')),
    '2017': (datetime.strptime('10/01/2016', '%m/%d/%Y'), datetime.strptime('9/30/2017', '%m/%d/%Y')),
    '2016': (datetime.strptime('10/01/2015', '%m/%d/%Y'), datetime.strptime('9/30/2016', '%m/%d/%Y')),
    '2015': (datetime.strptime('10/01/2014', '%m/%d/%Y'), datetime.strptime('9/30/2015', '%m/%d/%Y'))
}


class Gsa_Key_Rate(object):
    """Store rate information from a GSA defined location."""

    def __init__(self, county, destination, rates):
        """ctor."""
        self.county = county
        self.destination = destination
        self.rates = rates

    @staticmethod
    def decode_gsa_rate(dct):
        """Decode stored GSA rates for locations."""
        if 'county' in dct:
            return Gsa_Key_Rate(dct['county'], dct['destination'], dct['rates'])
        return dct

    @staticmethod
    def load_location_rates(json_path):
        """Load GSA location rates into an object."""
        with open(json_path, 'r') as json_file:
            rate_tables = json.load(json_file, object_hook=Gsa_Key_Rate.decode_gsa_rate)

        return rate_tables


def save_tables(table, json_path=RATES_2017):
    with open(json_path, 'w') as f_out:
        f_out.write(json.dumps(table, sort_keys=True, indent=4))


def load_tables(json_path=RATES_2017):
    with open(json_path, 'r') as json_file:
        rate_tables = json.load(json_file)

    return rate_tables


def parse_gsa_table(rate_key, tbody_text, year):
    rate_dates = RATE_YEARS[year]

    record = None
    if len(tbody_text) == 15:
        record = make_table_record(rate_key, tbody_text, rate_dates)
    elif len(tbody_text) == 30:
        record1 = make_table_record(rate_key, tbody_text[:15], rate_dates)
        record2 = make_table_record(rate_key, tbody_text[15:], rate_dates)
        if sum(record1[rate_key]['rates'].values()) > sum(record2[rate_key]['rates'].values()):
            record = record1
        else:
            record = record2
        record[rate_key]['category'] = RECORD_CATEGORIES['TWO_TABLE']

    return record


def make_table_record(rate_key, tbody_text, rate_dates):
    record = {
        rate_key: {
            'destination': tbody_text[0],
            'county': tbody_text[1],
            'rates': None
        }
    }
    rates = tbody_text[2:-1]
    if len(rates) != 12:
        raise ValueError('Where are all the rates!!!')
    rates = dict(zip(rate_dates, [int(r.replace('$', '').strip()) for r in rates]))
    record[rate_key]['rates'] = rates

    return record


def get_rate_key(state, city, zipcode):
    rate_key = '{}:{}'.format(state.lower(), city.lower() + str(zipcode))
    return rate_key


def request_gsa_destination(state, city, zipcode, fiscal_year):
    gsa_url = "https://inventory.data.gov/api/action/datastore_search"
    payload = {
        'resource_id': '8ea44bc4-22ba-4386-b84c-1494ab28964b',
        'limit': 20,
        'filters': '{{"FiscalYear":{},"Zip":{}}}'.format(fiscal_year, zipcode)
    }
    r = requests.get(gsa_url, params=payload)
    print(r.json())

def get_perdiem_table(state, city, zipcode, previous_tables, gsa_multitables, fiscal_year):
    global TABLE_YEAR_PATHS, RATE_YEAR_PATHS
    # https://www.gsa.gov/travel/plan-book/per-diem-rates/per-diem-rates-lookup/?action=perdiems_report&state=AL&fiscal_year=2017&zip=&city=mobile
    state = lookup_state(state)
    rate_key = get_rate_key(state, city, zipcode)
    if rate_key in previous_tables:
        previous_tables[rate_key]
        return previous_tables[rate_key]
    # elif rate_key in gsa_multitables:
    #     return None

    time.sleep(random.uniform(2.0, 5.0))
    apiCheck_Url = "https://www.gsa.gov/travel/plan-book/per-diem-rates/per-diem-rates-lookup"
    payload = {
        'action': 'perdiems_report',
        'state': state.upper(),
        'fiscal_year': fiscal_year,
        'zip': zipcode,
        'city': city.lower(),
    }
    r = requests.get(apiCheck_Url, params=payload)
    print(r.url)
    page = None

    try:
        page = BeautifulSoup(r.content, 'html.parser')
    except:
        print("Error: Service did not respond.")
    error_divs = page.find_all('div', {'class': 'error-text-body'})
    if len(error_divs) > 0:
        error_tag = error_divs[0].find_all('h3')
        error_text = ''
        if error_tag is not None and type(error_tag) == element.ResultSet:
            error_text = error_tag[0].text

        print('Error:', error_text)
        return None
    try:
        cells = []
        for tr in page.find_all('tr')[1:]:
            cells.extend(tr.find_all('td'))
        tbody_text = [e.get_text() for e in cells]
        if len(tbody_text) > 30:
            print('More than two tables. Cell count: {}'.format(len(tbody_text)))
            return None
    except IndexError:
        print('Odd page on:', r.url)
        return None
    try:
        table_record = parse_gsa_table(rate_key, tbody_text, fiscal_year)
    except ValueError:
        print('MakeTableIssue', rate_key)
        return None

    if table_record is None:
        gsa_multitables[rate_key] = tbody_text
        save_tables(gsa_multitables, TABLE_YEAR_PATHS[fiscal_year])
        return None

    previous_tables[rate_key] = table_record[rate_key]
    save_tables(previous_tables, RATE_YEAR_PATHS[fiscal_year])
    return table_record[rate_key]


def get_fiscal_year(month_day_year):
    date = datetime.strptime(month_day_year, '%m/%d/%Y')
    for year in FISCAL_YEARS:
        start = FISCAL_YEARS[year][0]
        end = FISCAL_YEARS[year][1]
        if date >= start and date <= end:
            return year

    return None


def lookup_state(state):
    state_codes = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
    }
    return state_codes[state]


def log_error(id_num, category):
    error_csv = 'results/errors.csv'
    with open(error_csv, 'ab') as errors:
        writer = csv.writer(errors)
        writer.writerow((id_num, category))


def get_records_from_table(data, previous_tables, fiscal_year):
    global TABLE_YEAR_PATHS
    gsa_multitables = {}
    if os.path.exists(TABLE_YEAR_PATHS[fiscal_year]):
        gsa_multitables = load_tables(TABLE_YEAR_PATHS[fiscal_year])

    date_string = '%m/%d/%Y'
    with open(data, 'rb') as stays:
        reader = csv.DictReader(stays)

        # with open('stays/non_utah_last_states.csv', 'wb') as fun:
        #     funwriter = csv.writer(fun)
        #     funwriter.writerow(reader.fieldnames)

        for row in reader:
            id_num, state, city, zipcode, checkin_date = (
                                                  int(row['ROW_ID'].strip()),
                                                  row['STATE'].strip(),
                                                  row['CITY'].strip(),
                                                  row['ZIP_CODE'].strip(),
                                                  row['CHECKIN_DATE'].strip())
            try:
                state = lookup_state(state)
                if state == 'Utah':
                    continue

            except KeyError:
                print('NOT FOUND', id_num, state, zipcode)
                log_error(id_num, 'state not found')
                continue

            try:
                stay_year = get_fiscal_year(checkin_date)
                if stay_year != fiscal_year:
                    continue
            except ValueError:
                print('BAD CHECKIN', id_num, checkin_date)
                log_error(id_num, 'checkin_date format error')
                continue
            # checkin_date = datetime.strftime(datetime.strptime(checkin_date, date_string), RATE_DATE_FORMAT)
            city, zipcode = (city, zipcode)
            zip1 = ''
            if ZIP_MATCHER.match(zipcode) is not None:
                zip1 = zipcode.split('-')[0].strip()
            record = get_perdiem_table(row['STATE'].strip(), city, zip1, previous_tables, gsa_multitables, fiscal_year)
            if record is None:
                record = get_perdiem_table(row['STATE'].strip(), city, '', previous_tables, gsa_multitables, fiscal_year)
                if record is None:
                    # log_error(id_num, 'service error')
                    continue


def get_records_from_lookup(data, previous_tables, output_csv, fiscal_year):
    date_string = '%m/%d/%Y'
    non_year = 0
    non_twotable = 0
    with open(data, 'rb') as stays, open(output_csv, 'wb') as output:
        reader = csv.DictReader(stays)
        writer = csv.writer(output)
        if 'PERDIEM' not in reader.fieldnames:
            writer.writerow(reader.fieldnames + ['PERDIEM'])
        else:
            writer.writerow(reader.fieldnames)

        for row in reader:
            id_num, state, city, zipcode, checkin_date = (
                                                  int(row['ROW_ID'].strip()),
                                                  row['STATE'].strip(),
                                                  row['CITY'].strip(),
                                                  row['ZIP_CODE'].strip(),
                                                  row['CHECKIN_DATE'].strip())
            try:
                state = lookup_state(state)
                if state == 'Utah':
                    continue

            except KeyError:
                print('NOT FOUND', id_num, state, zipcode)
                continue
            try:
                stay_year = get_fiscal_year(checkin_date)
                if stay_year != fiscal_year:
                    if stay_year != fiscal_year:
                        non_year += 1
                    continue
            except ValueError:
                print('BAD CHECKIN', id_num, checkin_date)
                continue
            rate_date = datetime.strftime(datetime.strptime(checkin_date, date_string), RATE_DATE_FORMAT)
            city, zipcode = (city, zipcode)
            zip1 = ''
            if ZIP_MATCHER.match(zipcode) is not None:
                zip1 = zipcode.split('-')[0].strip()

            rate_key = get_rate_key(state, city, zip1)
            zipless_key = get_rate_key(state, city, '')
            if rate_key not in previous_tables:
                if zipless_key not in previous_tables:
                    non_twotable += 1
                    continue
                else:
                    rate_key = zipless_key
            rates = previous_tables[rate_key]['rates']
            # print(id_num, state, city, zipcode, checkin_date)
            if rate_date not in rates:
                print('bad rate date', rate_key, rate_date)
            else:
                row['PERDIEM'] = rates[rate_date]
                writer.writerow([row[field] for field in reader.fieldnames])  # + [rates[rate_date]])
    print('Wrong year: {} Non two table: {}'.format(non_year, non_twotable))


def remove_completed(stay_csv, completed_csv, shared_id_field, output_csv):
    completed_ids = {}
    with open(completed_csv) as completed:
        completed_reader = csv.DictReader(completed)
        for row in completed_reader:
            completed_ids[row[shared_id_field].replace('`', '')] = None

    with open(stay_csv, 'rb') as stays, open(output_csv, 'wb') as output:
        reader = csv.DictReader(stays)
        writer = csv.writer(output)
        writer.writerow(reader.fieldnames)
        for row in reader:
            id_num = row[shared_id_field]
            if id_num not in completed_ids:
                writer.writerow([row[field] for field in reader.fieldnames])


def _transform_rate_dates(rates_json, temp_rates_json):
    rate_tables = load_tables(rates_json)
    for key in rate_tables:
        new_rates = {}
        rates = rate_tables[key]['rates']
        for date in rates:
            new_date = date.replace('2016', '2015').replace('2017', '2016')
            new_rates[new_date] = rates[date]

        rate_tables[key]['rates'] = new_rates
    save_tables(rate_tables, temp_rates_json)


def _transform_rate_dates(rates_json, temp_rates_json):
    rate_tables = load_tables(rates_json)
    for key in rate_tables:
        new_rates = {}
        rates = rate_tables[key]['rates']
        for date in rates:
            new_date = date.replace('2016', '2015').replace('2017', '2016')
            new_rates[new_date] = rates[date]

        rate_tables[key]['rates'] = new_rates
    save_tables(rate_tables, temp_rates_json)


def _combine_result_tables(result_folder, csv_tables, output_csv):
    fields = None
    with open(csv_tables[0], 'rb') as result:
        reader = csv.DictReader(result)
        fields = reader.fieldnames

    with open(output_csv, 'wb') as output:
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)
        writer.writerow(fields)
        total_rows = 0
        for table in csv_tables:
            with open(table, 'rb') as t:
                reader = csv.reader(t)
                reader.next()
                for row in reader:
                    total_rows += 1
                    writer.writerow(['\'' + v if v.startswith('00') else v for v in row])
        print('Total result rows:', total_rows)


def check_utah_cities():
    perdiem_csv = r'stays/utah_perdiems.csv'
    utah_stay_csv = r'stays/utah_stays.csv'
    perdiem_cities = {}
    with open(perdiem_csv, 'rb') as p_cities:
        reader = csv.DictReader(p_cities)
        print(reader.fieldnames)
        for row in reader:
            perdiem_cities[row['CITY'].lower().strip()] = None

    with open(utah_stay_csv, 'rb') as u_cities:
        reader = csv.DictReader(u_cities)
        count = 0
        for row in reader:
            utah_city = row['CITY'].lower().strip()
            if utah_city not in perdiem_cities:
                if utah_city.replace('city', '').strip() not in perdiem_cities:
                    # print(utah_city)
                    count += 1
        print(count)


def _get_random_sample(records, n, skip_utah=False):
    low_id = 1
    high_id = 1514
    import random
    record_ids = {}
    sample = set()
    with open(records, 'rb') as r:
        reader = csv.DictReader(r)
        for row in reader:
            if skip_utah and lookup_state(row['STATE']) == 'Utah':
                continue

            record_ids[row['ROW_ID'].replace('`', '')] = '{}, {} {}\nYear: {} \nRate: {}'.format(
                row['CITY'],
                lookup_state(row['STATE']),
                row['ZIP_CODE'].replace('`', ''),
                row['CHECKIN_DATE'].strip(),
                row['PERDIEM'].replace('`', ''))

    while len(sample) <= n:
        try:
            id_num = random.randint(low_id, high_id)
            print(len(sample), record_ids[str(id_num)])
            sample.add(record_ids[str(id_num)])

        except KeyError:
            continue

    return sample


def run_table(data, year, output_csv):
    previous_tables = {}
    if os.path.exists(RATE_YEAR_PATHS[year]):
        previous_tables = Gsa_Key_Rate.load_location_rates(RATE_YEAR_PATHS[year])

    get_records_from_table(data, previous_tables, year)
    get_records_from_lookup(data, previous_tables, output_csv, year)


def find_missing_records(stays, results):
    result_ids = set()
    stay_ids = set()
    with open(results, 'rb') as r:
        results_reader = csv.DictReader(r)
        for row in results_reader:
            result_ids.add(int(row['ROW_ID'].replace('\'', '')))
    with open(stays, 'rb') as s:
        reader = csv.DictReader(s)
        for row in reader:
            stay_ids.add(int(row['ROW_ID'].replace('\'', '')))
    for stay_id in stay_ids:
        if stay_id not in result_ids:
            print('missing stay', stay_id)
    for result_id in result_ids:
        if result_id not in stay_ids:
            print('result', result_id)
    print
    print('total results', len(result_ids))
    print('total stays', len(stay_ids))


def combine_data_to_bq():
    tables = [
        ('results/non_utah_2015.csv', 'results/non_utah_2015.csv')
    ]


if __name__ == '__main__':
    """Currently uses web_scraping python virtual env"""
    data = 'stays/All_Stays_2018Q4.csv'
    year = '2018'
    quarter = 'q4'

    output_suffix = year + '_' + quarter
    non_utah_output = 'results/non_utah_{}.csv'.format(output_suffix)
    print('\n!!!!!US stays!!!!!!!')
    run_table(data, year, non_utah_output)
    # Run utah_perdiems.py
    from utah_perdiem import create_rate_areas
    from utah_perdiem import get_rate_for_stays
    utah_perdiems_csv = r'stays/utah_perdiems.csv'
    utah_output = 'results/utah_{}.csv'.format(output_suffix)
    print('\n!!!!!Utah Stays!!!!!!!')
    city_areas = create_rate_areas(utah_perdiems_csv)
    get_rate_for_stays(city_areas, data, utah_output)
    #
    # Combine non_utah and utah results
    print('\n!!!!!Combine!!!!!!!')
    result_folder = 'results'
    csv_tables = [non_utah_output, utah_output]
    combined_output = 'results/results_{}.csv'.format(output_suffix)
    _combine_result_tables(result_folder, csv_tables, combined_output)
    print('Results at {}'.format(combined_output))
    find_missing_records(data, combined_output)

    # _get_random_sample(combined_output, 30)
