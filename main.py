import logging
import os

from requests import HTTPError

from parse import BRAHMSExportReader, brahms_row_to_payload, construct_img_filepath, extract_species_info
from post import RBGAPIPoster

logging.basicConfig(filename='main.log', level=logging.ERROR)
logger = logging.getLogger(__name__)


def post_plant_collections(poster):
    sql_reader = BRAHMSExportReader(file_path='living_plant_collections.csv', encoding='utf-16le', delimiter='|')

    sql_rows = iter(sql_reader.get_rows())
    next(sql_rows)  # Skip header row
    for row in sql_rows:
        payload = brahms_row_to_payload(row)
        #print(payload)
        try:
            resp = poster.post_collection(payload)
        except HTTPError as e:
            print(e.response.text)
            logger.error(e)
            logger.error(e.response.text)
            logger.error(payload)


def post_image_to_species(poster):
    image_location_reader = BRAHMSExportReader(file_path='species_image_locations.csv')

    for row in image_location_reader.get_rows():
        img_filepath = construct_img_filepath(row)
        species_image_payload = extract_species_info(row)
        resp = poster.get_species_from_query(species_image_payload)
        content = resp.json()

        if content['count'] == 1:
            species_pk = content['results'][0]['id']

            poster.post_species_image(species_pk, img_filepath)


def main():
    username = os.environ.get('RBG_API_USERNAME')
    password = os.environ.get('RBG_API_PASSWORD')
    poster = RBGAPIPoster(username=username, password=password, netloc='localhost:8000', ssl=False)

    post_plant_collections(poster)
    post_image_to_species(poster)


if __name__ == '__main__':
    main()
