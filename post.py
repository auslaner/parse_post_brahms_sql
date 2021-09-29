import logging
import sys
from urllib.parse import urlunsplit

import requests

logger = logging.getLogger(__name__)


class RBGAPIPoster:
    def __init__(self, username, password, netloc='redbuttegarden.org:80', ssl=True):
        self.session = requests.session()
        self.username = username
        self.password = password
        self.netloc = netloc
        self.ssl = ssl
        self.scheme = self.get_scheme()
        tokens = self.get_tokens()
        self.session.headers = {'Accept': 'application/json; q=1.0, */*',
                                'X-CSRFToken': tokens['csrftoken'],
                                'Authorization': 'Token ' + tokens['drf_token']}

    def get_tokens(self):
        url = urlunsplit((self.scheme, self.netloc, '/plants/api/token/', '', ''))
        response = self.session.post(url, data={'username': self.username, 'password': self.password})
        try:
            assert response.status_code == 200
        except AssertionError:
            logger.error("Failed to get token. Are you sure a user with these credentials exists on the website?")
            sys.exit(1)
        tokens = {'csrftoken': response.cookies['csrftoken'],
                  'drf_token': response.json()['token']}
        return tokens

    def get_scheme(self):
        if self.ssl:
            return 'https'
        else:
            return 'http'

    def get_species_from_query(self, payload):
        url = urlunsplit((self.scheme, self.netloc, '/plants/api/species/', '', ''))

        r = self.session.get(url, params=payload)
        r.raise_for_status()
        return r

    def post_collection(self, payload):
        url = urlunsplit((self.scheme, self.netloc, '/plants/api/collections/', '', ''))

        r = self.session.post(url, json=payload)
        r.raise_for_status()
        return r

    def post_species_image(self, pk, file_path, copyright_info):
        url = urlunsplit((self.scheme, self.netloc, f'/plants/api/species/{pk}/set-image/', '', ''))

        with open(file_path, 'rb') as f:
            r = self.session.post(url, data={'copyright_info': copyright_info}, files={'image': f})
            r.raise_for_status()
            return r
