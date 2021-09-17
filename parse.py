import csv
import json
import os


def process_bloom_time(bloom_time_string):
    """
    Takes bloom time returned from the bloomtime column of the
    BRAHMS SQL export and converts it into a list that is
    acceptable by the bloom_time ArrayField of the Species model
    on the RBG website.
    :param bloom_time_string: Tenth element of a row returned by
    SQLExportReader.get_rows()
    :return: List of month strings representing bloom times.
    """
    conversion = {
        'Jan': 'January',
        'Feb': 'February',
        'Mar': 'March',
        'Apr': 'April',
        'Arp': 'April',  # Common misspelling
        'MAy': 'May',
        'May': 'May',
        'Jun': 'June',
        'Jul': 'July',
        'Aug': 'August',
        'Sep': 'September',
        'Sept': 'September',
        'Oct': 'October',
        'Nov': 'November',
        'Dec': 'December'
    }
    month_list = []

    for month_string in bloom_time_string.split(', '):
        try:
            month_list.append(conversion[month_string.strip()])
        except KeyError:
            # Probably missing a space between comma
            for fixed_month_string in month_string.split(','):
                if fixed_month_string:
                    try:
                        month_list.append(conversion[fixed_month_string.strip()])
                    except KeyError as e:
                        print(e)

    return month_list


def process_plant_date(day, month, year):
    try:
        if (int(day) in range(1, 31)) and \
                (int(month) in range(1, 12)) and \
                (len(year) == 4):

            return '-'.join([day, month, year])
    except ValueError:
        return None


def clean_row(row):
    """Remove trailing commas from row data"""
    cleaned_data = []
    for row_data in row:
        cleaned_data.append(row_data.strip(','))

    return cleaned_data


def process_hardiness(hardiness_data):
    """
    Make sure all elements can be coerced to integers.
    :param hardiness_data: String list representing hardiness zones
    :return: List of integers
    """
    clean_hardiness = []
    for elem in hardiness_data.split(','):
        try:
            hardiness = int(elem.strip())
            clean_hardiness.append(hardiness)
        except ValueError:
            continue

    return clean_hardiness


def brahms_row_to_payload(row):
    row = clean_row(row)
    hardiness = process_hardiness(row[7])
    bloom_times = process_bloom_time(row[10]) if row[10] else []
    plant_date = process_plant_date(day=row[21], month=row[22], year=row[23]) \
        if (row[21] and row[22] and row[23]) else None
    payload = {
        "species": {
            "genus": {
                "family": {
                    "name": row[0],
                    "vernacular_name": row[1]
                },
                "name": row[2]
            },
            "name": row[3],
            "cultivar": row[4],
            "vernacular_name": row[5],
            "habit": row[6],
            "hardiness": hardiness,
            "water_regime": row[8],
            "exposure": row[9],
            "bloom_time": bloom_times,
            "plant_size": row[11],
            "flower_color": row[12],
            "utah_native": True if row[26] in ['Yes', 'yes', 'x', 'Utah Native'] else False,
            "plant_select": True if row[27] in ['Yes', 'yes', 'x'] else False,
            "deer_resist": True if row[28] in ['Yes', 'yes', 'x'] else False,
            "rabbit_resist": True if row[29] in ['Yes', 'yes', 'x'] else False,
            "bee_friend": True if row[30] in ['Yes', 'yes', 'x'] else False
        },
        "garden": {
            "area": row[13],
            "name": row[14],
            "code": row[15]
        },
        "location": {
            "latitude": round(float(row[17]), 6),
            "longitude": round(float(row[18]), 6)
        },
        "plant_date": plant_date,
        "plant_id": row[16],
        "commemoration_category": row[19],
        "commemoration_person": row[20]
    }

    return payload


def construct_img_filepath(row):
    return os.path.join(row[1], row[0].replace('\ufeff', ''))


def convert_to_json(dictionary):
    return json.dumps(dictionary)


def extract_species_info(row):
    payload = {
        "name": row[3] if row[3] != 'NULL' else None,
        "cultivar": row[4] if row[4] != 'NULL' else None,
        "vernacular_name": row[5] if row[5] != 'NULL' else None,
        "genus": row[2]
    }

    return payload


class BRAHMSExportReader:
    """
    Reads and parses SQL Export from BRAHMS data tables.
    """
    def __init__(self, file_path, encoding='utf-8', delimiter=','):
        self.file_path = file_path
        self.encoding = encoding
        self.delimiter = delimiter

    def get_rows(self):
        with open(self.file_path, encoding=self.encoding) as csvfile:
            reader = csv.reader(csvfile, delimiter=self.delimiter)
            for row in reader:
                yield row
