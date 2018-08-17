"""Create perdiem data for Utah hotel stays."""
import csv
import os
from datetime import datetime


class RateArea(object):
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


def check_utah_cities():
    perdiem_csv = r'stays/utah_perdiems.csv'
    utah_stay_csv = r'stays/utah_stays.csv'
    perdiem_cities = {}
    with open(perdiem_csv, 'rb') as p_cities:
        reader = csv.DictReader(p_cities)
        print reader.fieldnames
        for row in reader:
            perdiem_cities[row['CITY'].lower().strip()] = None

    with open(utah_stay_csv, 'rb') as u_cities:
        reader = csv.DictReader(u_cities)
        count = 0
        for row in reader:
            utah_city = row['CITY'].lower().strip()
            if utah_city not in perdiem_cities:
                if utah_city.replace('city', '').strip() not in perdiem_cities:
                    # print utah_city
                    count += 1
        print count


def create_rate_areas(perdiem_csv):
    date_string = '%m/%d/%Y'
    with open(perdiem_csv, 'rb') as p_cities:
        reader = csv.DictReader(p_cities)
        for row in reader:
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
    date_string = '%m/%d/%Y'
    not_found = 0
    not_found_cities = {}
    default_rate = 70
    with open(stay_csv, 'rb') as stays, open(output_csv, 'wb') as output:
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
                # print city, 'not found'
                not_found_cities[city] = 'not found'
                row['PERDIEM'] = default_rate
                writer.writerow([row[field] for field in reader.fieldnames])
                not_found += 1
            else:
                rate = city_areas[city].get_rate(checkin)
                if rate is None:
                    # print city, 'date not not_found', checkin
                    not_found_cities[city] = 'date not not_found ' + str(checkin)
                    row['PERDIEM'] = default_rate
                    writer.writerow([row[field] for field in reader.fieldnames])
                    not_found += 1
                else:
                    row['PERDIEM'] = int(float(rate))
                    writer.writerow([row[field] for field in reader.fieldnames])

    for not_found_city, msg in not_found_cities.items():
        print '{} {}'.format(not_found_city, msg)
    print 'Total not found:', not_found


if __name__ == '__main__':
    perdiem_csv = r'stays/utah_perdiems.csv'
    utah_stay_csv = r'stays/All_Stays_2018Q2.csv'
    city_areas = create_rate_areas(perdiem_csv)
    get_rate_for_stays(city_areas, utah_stay_csv, 'results/utah_2018_q2.csv')
    # print len(city_areas)
