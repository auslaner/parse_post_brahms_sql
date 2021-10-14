import csv
import json
import logging
import os
import sys

logger = logging.getLogger(__name__)


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
                    except KeyError:
                        logger.error('KeyError while processing:\n' + bloom_time_string)
                        raise

    return month_list


def process_plant_date(day, month, year):
    try:
        if (int(day) in range(1, 31)) and \
                (int(month) in range(1, 12)) and \
                (len(year) == 4):

            return '-'.join([day, month, year])
    except ValueError:
        logger.error(f'ValueError while processing:\nDay:{day}, Month:{month}, Year:{year}')
        raise


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
            raise

    return clean_hardiness


def get_column_mapping(row):
    """
    Assumes row structure:
    familyname|vernacularfamilyname|genusname|speciesname|subspecies|variety|subvariety|forma|subforma|cultivar|vernacularname|habit|hardiness|waterregime|exposure|bloomtime|plantsize|colour|gardenlocalityarea|gardenlocalityname|gardenlocalitycode|plantid|latitude|longitude|commemorationcategory|commemorationperson|plantday|plantmonth|plantyear|notonline|lastmodifiedon|str12|str18|str19|str20|str22|str23
    """
    column_mapping = {
        'familyname': row[0],
        'vernacularfamilyname': row[1],
        'genusname': row[2],
        'speciesname': row[3],
        'subspecies': row[4],
        'variety': row[5],
        'subvariety': row[6],
        'forma': row[7],
        'subforma': row[8],
        'cultivar': row[9],
        'vernacularname': row[10],
        'habit': row[11],
        'hardiness': row[12],
        'waterregime': row[13],
        'exposure': row[14],
        'bloomtime': row[15],
        'plantsize': row[16],
        'colour': row[17],
        'gardenlocalityarea': row[18],
        'gardenlocalityname': row[19],
        'gardenlocalitycode': row[20],
        'plantid': row[21],
        'latitude': row[22],
        'longitude': row[23],
        'commemorationcategory': row[24],
        'commemorationperson': row[25],
        'plantday': row[26],
        'plantmonth': row[27],
        'plantyear': row[28],
        'notonline': row[29],
        'lastmodified': row[30],
        'utahnative': row[31],  # str12
        'plantselect': row[32],  # str18
        'deer': row[33],  # str19
        'rabbit': row[34],  # str20
        'bee': row[35],  # str22
        'highelevation': row[36]  # str23
    }

    return column_mapping


def brahms_row_to_payload(row):
    row = clean_row(row)

    column_mapping = get_column_mapping(row)

    plant_id = column_mapping['plantid']

    try:
        hardiness = process_hardiness(column_mapping['hardiness']) if column_mapping['hardiness'] else []
    except ValueError:
        logger.error(f"Failed to process hardiness for collection with ID {plant_id}: {column_mapping['hardiness']}")
        return None

    try:
        bloom_times = process_bloom_time(column_mapping['bloomtime']) if column_mapping['bloomtime'] else []
    except KeyError:
        logger.error(f"Failed to process bloom time for collection with ID {plant_id}: column_mapping['bloomtime']")
        return None

    try:
        day = column_mapping['plantday']
        month = column_mapping['plantmonth']
        year = column_mapping['plantmonth']
        plant_date = process_plant_date(day=day, month=month, year=year) if (day and month and year) else None
    except ValueError:
        logger.error(f'Failed to process date for collection with ID {plant_id}')
        return None

    payload = {
        'species': {
            'genus': {
                'family': {
                    'name': column_mapping['familyname'],
                    'vernacular_name': column_mapping['vernacularfamilyname']
                },
                'name': column_mapping['genusname']
            },
            'name': column_mapping['speciesname'],
            'subspecies': column_mapping['subspecies'],
            'variety': column_mapping['variety'],
            'subvariety': column_mapping['subvariety'],
            'forma': column_mapping['forma'],
            'subforma': column_mapping['subforma'],
            'cultivar': column_mapping['cultivar'],
            'vernacular_name': column_mapping['vernacularname'],
            'habit': column_mapping['habit'],
            'hardiness': hardiness,
            'water_regime': column_mapping['waterregime'],
            'exposure': column_mapping['exposure'],
            'bloom_time': bloom_times,
            'plant_size': column_mapping['plantsize'],
            'flower_color': column_mapping['colour'],
            'utah_native': True if column_mapping['utahnative'].lower() in ['yes', 'x', 'utah native'] else False,
            'plant_select': True if column_mapping['plantselect'].lower() in ['yes', 'x'] else False,
            'deer_resist': True if column_mapping['deer'].lower() in ['yes', 'x'] else False,
            'rabbit_resist': True if column_mapping['rabbit'].lower() in ['yes', 'x'] else False,
            'bee_friend': True if column_mapping['bee'].lower() in ['yes', 'x'] else False,
            'high_elevation': True if column_mapping['highelevation'].lower() in ['yes', 'x'] else False,
        },
        'garden': {
            'area': column_mapping['gardenlocalityarea'],
            'name': column_mapping['gardenlocalityname'],
            'code': column_mapping['gardenlocalitycode']
        },
        'location': {
            'latitude': round(float(column_mapping['latitude']), 6),
            'longitude': round(float(column_mapping['longitude']), 6)
        },
        'plant_date': plant_date,
        'plant_id': plant_id,
        'commemoration_category': column_mapping['commemorationcategory'],
        'commemoration_person': column_mapping['commemorationperson']
    }

    return payload


def construct_img_filepath(row):
    """
    Assumes row structure: imagefile|copyright|directoryname|genusname|speciesname|cultivar|vernacularname
    :param row:
    :return:
    """
    if len(row) < 6:
        logger.error(f'Invalid row: {row}')
        raise ValueError

    # If we're not running on the VM with the mapped drive, change the filepath to Box
    if sys.platform == 'darwin':
        box_path = os.path.join(os.path.expanduser('~'), 'Box', 'RBG-Shared', 'Photo Library - Plant Records',
                                'AA BRAHMS Resized Photos', '')
        path_from_row = os.path.join(row[2].replace('B:\\', box_path), row[0].replace('\ufeff', ''))
    else:
        path_from_row = os.path.join(row[2], row[0].replace('\ufeff', ''))

    return path_from_row


def convert_to_json(dictionary):
    return json.dumps(dictionary)


def extract_species_info(row):
    """
    Assumes row structure: imagefile|copyright|directoryname|genusname|speciesname|cultivar|vernacularname
    """
    payload = {
        'name': row[4] if row[4] != 'NULL' else None,
        'cultivar': row[5] if row[5] != 'NULL' else None,
        'vernacular_name': row[6] if row[6] != 'NULL' else None,
        'genus': row[3]
    }

    return payload


def extract_copyright_info(row):
    """
    Assumes row structure: imagefile|copyright|directoryname|genusname|speciesname|cultivar|vernacularname
    """
    return row[1]


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
