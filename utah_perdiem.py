"""Create perdiem data for Utah hotel stays."""
import csv
import os
from datetime import datetime


class RateArea(object):
    "Store rate information for Utah cities."
    Areas = {}

    def __init__(self, name):
        self.name = name
        self._rate_periods = []
        RateArea.Areas[name] = self

    def add_rate_period(self, begin, end, rate):
        self._rate_periods.append((begin, end, rate))

    def get_rate(self, date):
        for period in self._rate_periods:
            begin, end, rate = period
            if date >= begin and date <= end:
                return rate

        return None


def create_rate_areas(perdiem_csv):
    """
    Create city rate areas and add rate date ranges.
    - perdiem_csv: csv of perdiem rates for Utah cities."""
    date_string = '%m/%d/%Y'
    with open(perdiem_csv, 'r') as p_cities:
        reader = csv.DictReader(p_cities)
        for row in reader:
            if row['STATE'] != 'UT':
                continue
            city = row['CITY'].lower().replace('city', '').strip()
            begin = datetime.strptime(row['BEG_DATE'].strip(), date_string)
            end = datetime.strptime(row['END_DATE'].strip(), date_string)
            rate = row['RATE'].replace('$', '').strip()
            if city not in RateArea.Areas:
                city_area = RateArea(city)
                city_area.add_rate_period(begin, end, rate)
            else:
                RateArea.Areas[city].add_rate_period(begin, end, rate)

    return RateArea.Areas


def get_rate_for_stays(city_areas, stay_csv, output_csv):
    """Add Utah travel rates to Utah hotel stays."""
    date_string = '%m/%d/%Y'
    not_found = 0
    not_found_cities = {}
    default_rate = 70  # This value can change each new fiscal year and can be found in rates csv.
    with open(stay_csv, 'r') as stays, open(output_csv, 'w', newline='') as output:
        reader = csv.DictReader(stays)
        writer = csv.writer(output)
        if 'PERDIEM' not in reader.fieldnames:
            writer.writerow(reader.fieldnames + ['PERDIEM'])
        else:
            writer.writerow(reader.fieldnames)
        for row in reader:
            if row['STATE'].lower() != 'ut':
                continue
            city = row['CITY'].lower().replace('city', '').strip()
            checkin = datetime.strptime(row['CHECKIN_DATE'].strip(), date_string)
            id_num = row['ROW_ID']
            if city not in city_areas:
                not_found_cities[city] = 'not found'
                row['PERDIEM'] = default_rate
                writer.writerow([row[field] for field in reader.fieldnames])
                not_found += 1
            else:
                rate = city_areas[city].get_rate(checkin)
                if rate is None:
                    not_found_cities[city] = 'date not not_found ' + str(checkin)
                    row['PERDIEM'] = default_rate
                    writer.writerow([row[field] for field in reader.fieldnames])
                    not_found += 1
                else:
                    row['PERDIEM'] = int(float(rate))
                    writer.writerow([row[field] for field in reader.fieldnames])

    for not_found_city, msg in not_found_cities.items():  # cities not found in Utah rates. All not found are Utah default rate.
        print('{} {}'.format(not_found_city, msg))
    print('Total not found:', not_found)

