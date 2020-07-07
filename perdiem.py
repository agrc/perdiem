"""Script to add federal and Utah hotel per diems to hotel stay data."""
import json
import requests
import csv
from datetime import datetime
import re
import random
import time
import os
import calendar

RATE_LIMIT_SECONDS = (0.1, 0.2)  # average request rate to GSA will be uniformly distributed between these times

ZIP_PLUS4_MATCHER = re.compile(r'\d{5}($|(-\d{4}))')

RATE_DATE_FORMAT = '%Y-%m'

FEDERAL_FISCAL_YEARS = {  # add new fiscal years in the future.
    '2020': (datetime.strptime('10/01/2019', '%m/%d/%Y'), datetime.strptime('9/30/2020', '%m/%d/%Y')),
    '2019': (datetime.strptime('10/01/2018', '%m/%d/%Y'), datetime.strptime('9/30/2019', '%m/%d/%Y')),
    '2018': (datetime.strptime('10/01/2017', '%m/%d/%Y'), datetime.strptime('9/30/2018', '%m/%d/%Y')),
    '2017': (datetime.strptime('10/01/2016', '%m/%d/%Y'), datetime.strptime('9/30/2017', '%m/%d/%Y')),
    '2016': (datetime.strptime('10/01/2015', '%m/%d/%Y'), datetime.strptime('9/30/2016', '%m/%d/%Y')),
    '2015': (datetime.strptime('10/01/2014', '%m/%d/%Y'), datetime.strptime('9/30/2015', '%m/%d/%Y'))
}

GSA_MONTHS = list(calendar.month_abbr)[1:]

DEFAULT_GSA_RECORDS = {  # This is the standard GSA rate and may change each fiscal year.
    '2020': {'City': 'Standard Rate', 'Dec': '94', 'Feb': '94', 'Zip': '82930', 'Aug': '94', 'Sep': '94', 'Apr': '94', 'Jun': '94', 'State': 'UT', 'Jul': '94', 'Meals': '55', 'County': '', 'May': '94', 'DestinationID': '0', 'Mar': '94', 'Jan': '94', 'LocationDefined': '', 'Nov': '94', '_id': 59374, 'Oct': '94', 'FiscalYear': '2020'}
}


def fiscal_year_month_convertor(fiscal_year, month_abbr):
    """Convert federal fiscal year and 3 letter month to YYYY-MM format."""
    year_adjust = None
    if month_abbr in ['Oct', 'Nov', 'Dec']:
        year_adjust = -1
    else:
        year_adjust = 0
    adjusted_year = int(fiscal_year) + year_adjust
    converted_ratedate = datetime.strftime(datetime.strptime('{}-{}'.format(adjusted_year, month_abbr), '%Y-%b'),
                                           RATE_DATE_FORMAT)
    return converted_ratedate


def api_retry(api_call):
    """Retry and api call if calling method returns None."""
    def retry(*args, **kwargs):
        response = api_call(*args, **kwargs)
        back_off = 1
        while response is None and back_off <= 8:
            time.sleep(back_off + random.random())
            response = api_call(*args, **kwargs)
            back_off += back_off
        return response
    return retry


class Gsa_Destination_Rate(object):
    """Store rate information from a GSA defined location."""
    request_key_rates = {}

    def __init__(self, city, county, state, zipcode, destination_id, fiscal_year, rates, request_key=None):
        """ctor."""
        self.city = city
        self.county = county
        self.state = state
        self.zipcode = zipcode
        self.destination_id = destination_id
        self.rates = rates
        self.fiscal_year = fiscal_year
        if request_key is None:
            self.request_key = get_rate_key(fiscal_year, zipcode, state)
        else:
            self.request_key = request_key
        Gsa_Destination_Rate.request_key_rates[self.request_key] = self

    @staticmethod
    def encode_destination(destination):
        """Encode a Gsa_Destination_Rate to json."""
        if isinstance(destination, Gsa_Destination_Rate):
            field_dict = destination.__dict__
            return field_dict
        else:
            type_name = destination.__class__.__name__
            raise TypeError('Object of type {} is not JSON serializable'.format(type_name))

    @staticmethod
    def decode_api_record(record):
        """Decode GSA rates from API response record."""
        fiscal_year = record['FiscalYear']
        rates = {fiscal_year_month_convertor(fiscal_year, key): int(record[key]) for key in record if key in GSA_MONTHS}
        gsa_rate = Gsa_Destination_Rate(record['City'],
                                        record['County'],
                                        record['State'],
                                        record['Zip'],
                                        record['DestinationID'],
                                        fiscal_year,
                                        rates)

        return gsa_rate


def save_tables(destination_rates, json_path):
    """Cache rates for a destination as they are returned from the GSA API."""
    with open(json_path, 'w') as f_out:
        f_out.write(json.dumps(destination_rates, sort_keys=True, indent=4, default=Gsa_Destination_Rate.encode_destination))


def load_tables(json_path):
    with open(json_path, 'r') as json_file:
        key_rates = json.load(json_file)

    for key_rate in key_rates.values():
        Gsa_Destination_Rate(**key_rate)

    return Gsa_Destination_Rate.request_key_rates


def get_rate_key(fiscal_year, zipcode, state):
    """Create a key to from unique parts of a GSA API request."""
    rate_key = '{}:{}:{}'.format(fiscal_year, zipcode, state.upper())
    return rate_key


def select_rate(records, city):   #def select_rate(records, city)
    """
    Select rate from GSA records.
    Selected from city name match first.
    Selected from highest summed rates second.
    """
    selected_record = None
    max_rate_sum = 0
    for record in records:
        rate_sum = sum([int(record[key]) for key in record if key in GSA_MONTHS])
        gsa_city = record['City'].lower()
        if city.lower() in gsa_city:
            selected_record = record
            return selected_record
        elif rate_sum > max_rate_sum:
            selected_record = record
            max_rate_sum = rate_sum

    return selected_record


@api_retry
def request_gsa_destination(state, city, zipcode, fiscal_year):
    """
    Make a request to the GSA API.
    GSA API doc: https://www.gsa.gov/technology/government-it-initiatives/digital-strategy/per-diem-apis/per-diem-api
    """
    gsa_url = f'https://api.gsa.gov/travel/perdiem/v2/rates/zip/{zipcode}/year/{fiscal_year}'     #old url"https://inventory.data.gov/api/action/datastore_search"
    headers = {'x-api-key':'zoXm1gdyKjNAr6SIIJnd42u9ZvVbNSZvuPTmW1zV'}

    r = requests.get(gsa_url, headers=headers)
    if r.status_code >= 500:
        return None
    elif r.status_code >= 400 and r.status_code < 500:
        msg = 'Bad response from GSA API: url: {} code: {}'.format(r.url, r.status_code)
        raise Exception(msg)
    return r.json()

def modify_gsa_response(gsa_records_raw, state, city, zipcode, fiscal_year):
    modified_gsa = {}

    for r in gsa_records_raw:
        modified_gsa[r['short']] = r['value']
    modified_gsa['State'] = state
    modified_gsa['City'] = city
    modified_gsa['DestinationID'] = ''
    modified_gsa['Zip'] = zipcode
    modified_gsa['FiscalYear'] = fiscal_year
    modified_gsa['County'] = ''
    return modified_gsa

def get_destination_rate(state, city, zipcode, fiscal_year):
    """Get GSA rates for a destination."""
    rate_key = get_rate_key(fiscal_year, zipcode, state)
    if rate_key in Gsa_Destination_Rate.request_key_rates:
        return Gsa_Destination_Rate.request_key_rates[rate_key]

    time.sleep(random.uniform(RATE_LIMIT_SECONDS[0], RATE_LIMIT_SECONDS[1]))
    gsa_response = request_gsa_destination(state, city, zipcode, fiscal_year)

    if 'rates' not in gsa_response:
        print('rates not in dct')
        msg = 'Bad GSA response for params: {}'.format([state, city, zipcode, fiscal_year])
        raise Exception(msg)
    if len(gsa_response['rates']) < 1:
        print('no rates returned')
        msg = f'No GSA rates returned: {zipcode} {fiscal_year}'
        raise Exception(msg)
    records_raw = gsa_response['rates'][0]['rate'][0]['months']['month']
    records = [modify_gsa_response(records_raw, state, city, zipcode, fiscal_year)]

    selected_record = select_rate(records, city)
    if selected_record is None:
        selected_record = DEFAULT_GSA_RECORDS[fiscal_year]
        print('Using default record for: ', [state, city, zipcode, fiscal_year])

    table_record = Gsa_Destination_Rate.decode_api_record(selected_record)
    table_record.request_key = rate_key
    return table_record


def get_fiscal_year(month_day_year):
    """Given a date return it's federal fiscal year."""
    date = datetime.strptime(month_day_year, '%m/%d/%Y')
    for year in FEDERAL_FISCAL_YEARS:
        start = FEDERAL_FISCAL_YEARS[year][0]
        end = FEDERAL_FISCAL_YEARS[year][1]
        if date >= start and date <= end:
            return year

    return None


def lookup_state(state):
    """Get full state name from postal abbrevation."""
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


def add_perdiem_from_gsa(data, output_csv):
    """Add GSA perdiem to hotel stays for non-Utah data."""
    date_string = '%m/%d/%Y'
    with open(data, 'r') as stays, open(output_csv, 'w', newline='') as output:
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
            print(id_num)
            try:
                if state == 'UT':  # Utah stays are run on separate utah specific rates.
                    continue

            except KeyError:
                print('NOT FOUND', id_num, state, zipcode)
                continue

            try:
                fiscal_year = get_fiscal_year(checkin_date)
            except ValueError:
                print('BAD CHECKIN', id_num, checkin_date)
                continue

            if ZIP_PLUS4_MATCHER.match(zipcode) is not None:
                zipcode = zipcode.split('-')[0].strip()
            try:
                destination = get_destination_rate(state, city, zipcode, fiscal_year)
            except Exception as e:
                print(e)
                continue
            rate_date = datetime.strftime(datetime.strptime(checkin_date, date_string), RATE_DATE_FORMAT)
            row['PERDIEM'] = destination.rates[rate_date]
            writer.writerow([row[field] for field in reader.fieldnames])  # write result row


def run_table(data, serialized_rates_json, output_csv):
    """Run the non-Utah stays."""
    if os.path.exists(rates_json):
        load_tables(rates_json)
    try:
        add_perdiem_from_gsa(data, output_csv)
    except Exception as e:
        save_tables(Gsa_Destination_Rate.request_key_rates, rates_json)
        raise e

    save_tables(Gsa_Destination_Rate.request_key_rates, rates_json)


def _combine_result_tables(result_folder, csv_tables, output_csv):
    fields = None
    data_rows = 0
    with open(csv_tables[0], 'r') as result:
        reader = csv.DictReader(result)
        fields = reader.fieldnames
        for row in reader:
            data_rows += 1

    with open(output_csv, 'w', newline='') as output:
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)
        writer.writerow(fields)
        total_rows = 0
        for table in csv_tables:
            with open(table, 'r') as t:
                reader = csv.reader(t)
                next(reader)
                # add arrayformulas into first data row for google sheet
                if total_rows == 0:
                    first_data_row = next(reader)
                    first_data_row[-5] = '=ARRAYFORMULA(If(ISBLANK($S2:$S),"", $R2:$R-$S2:$S))'
                    first_data_row[-4] = '=ARRAYFORMULA(TO_PERCENT(If(ISBLANK($S2:$S),"", $T2:$T/$S2:$S)))'
                    first_data_row[-3] = '=ARRAYFORMULA(If(ISBLANK($S2:$S),"", $T2:$T*$Q2:$Q))'
                    writer.writerow(['\'' + v if v.startswith('00') else v for v in first_data_row])
                    total_rows += 1

                for row in reader:
                    total_rows += 1
                    writer.writerow(['\'' + v if v.startswith('00') else v for v in row])
        print('Total result rows:', total_rows)


def _get_random_sample(records, n, skip_utah=False):
    import random
    record_ids = {}
    sample = set()
    sample_num = 1
    with open(records, 'r') as r:
        reader = csv.DictReader(r)
        for row in reader:
            if skip_utah and row['STATE'] == 'UT':
                continue

            record_ids[sample_num] = 'ROW_ID:{}\n{}, {} {}\nYear: {} \nRate: {}'.format(
                row['ROW_ID'].replace('`', ''),
                row['CITY'],
                lookup_state(row['STATE']),
                row['ZIP_CODE'].replace('`', ''),
                row['CHECKIN_DATE'].strip(),
                row['PERDIEM'].replace('`', ''))
            sample_num += 1

    while len(sample) <= n:
        try:
            id_num = random.randint(1, sample_num)
            sample.add(record_ids[id_num])

        except KeyError:
            continue

    return sample


def find_missing_records(stays, results):
    result_ids = set()
    stay_ids = set()
    with open(results, 'r') as r:
        results_reader = csv.DictReader(r)
        for row in results_reader:
            result_ids.add(int(row['ROW_ID'].replace('\'', '')))
    with open(stays, 'r') as s:
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


if __name__ == '__main__':
    """Currently uses web-scraping3 python virtual env"""
    data = 'stays/All_Stays_2020Q3.csv'
    # Year and quarter for output file naming
    utah_fiscal_year = '2020'
    quarter = 'q3'

    output_suffix = utah_fiscal_year + '_' + quarter
    non_utah_output = 'results/non_utah_{}.csv'.format(output_suffix)
    print('\n!!!!!US stays!!!!!!!')
    rates_json = 'gsa_destination_rates/rates_{}.json'.format(utah_fiscal_year)
    run_table(data, rates_json, non_utah_output)

    # Run utah_perdiems.py
    from utah_perdiem import create_rate_areas
    from utah_perdiem import get_rate_for_stays
    utah_perdiems_csv = r'utah_rates.csv'
    utah_output = 'results/utah_{}.csv'.format(output_suffix)
    print('\n!!!!!Utah Stays!!!!!!!')
    city_areas = create_rate_areas(utah_perdiems_csv)
    get_rate_for_stays(city_areas, data, utah_output)

    # Combine non_utah and utah results
    print('\n!!!!!Combine!!!!!!!')
    result_folder = 'results'
    csv_tables = [non_utah_output, utah_output]
    combined_output = 'results/results_{}.csv'.format(output_suffix)
    _combine_result_tables(result_folder, csv_tables, combined_output)
    print('Results at {}'.format(combined_output))
    find_missing_records(data, combined_output)

    # Get a random sample of record for verification.
    print()
    print('----Verification random sample----')
    sample = _get_random_sample(combined_output, 10, skip_utah=False)
    for rate in sample:
        print(rate)
        print()