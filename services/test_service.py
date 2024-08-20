import requests
import unittest
import logging
import pandas as pd


URL = 'http://127.0.0.1:8000/'
HEADERS = {'Content-type': 'application/json', 'Accept': 'text/plain'}

logging.basicConfig(
    level=logging.INFO, 
    filename = "services/test_service.log", 
    format = "%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s", 
    datefmt='%H:%M:%S',
    )

catalog_names = pd.read_parquet('files/catalog_names.parquet')
items = pd.read_parquet('files/items.parquet')

def search(column, list_of_ids):
    if type(list_of_ids) is not list:
        list_of_ids = [list_of_ids]
    return ', '.join([catalog_names[(catalog_names['type']==column)&(catalog_names['id']==id)]['name'].values[0] for id in list_of_ids])

def make_report(list_of_tracks):
    dataset = pd.DataFrame(list_of_tracks, columns=['track_id']).merge(items, on='track_id').dropna()
    return '\n'.join([' | '.join([str(i+1), search('track', dataset.iloc[i]['track_id'].tolist()), search('artist', dataset.iloc[i]['artists'].tolist()), 
                      search('genre', dataset.iloc[i]['genres'].tolist())]) for i in range(len(dataset))])

logger=logging.getLogger()

def recommendations(params:dict) -> dict:
    resp = requests.post(URL+'recommendations', headers=HEADERS, params=params)
    return resp
    
class RecommenderTestCase(unittest.TestCase):
    # для пользователя с персональными рекомендациями, но без онлайн-истории
    def test_endpoint(self):
        logger.info("testing root endpoint")
        resp = requests.get(URL)
        self.assertEqual(resp.status_code, 200)
        logger.info(f"Got status {resp.status_code}")

    def test_only_personal(self):
        params = {"user_id": 1}
        logger.info(f"Testing the ouput of recommendations for user_id = {params['user_id']}, with personal recommendations, without a history")
        res = recommendations(params)
        self.assertEqual(res.status_code, 200)
        logger.info(f"Got status {res.status_code}")
        logger.info(f"recommendations for user_id = {params['user_id']}:\n{make_report(res.json()['recs'])}")

    # для пользователя с персональными рекомендациями и онлайн-историей
    def test_personal_online(self):
        params = {"user_id": 0}
        logger.info(f"Testing the ouput of recommendations for user_id = {params['user_id']}, with personal recommendations, with a history")
        res = recommendations(params)
        self.assertEqual(res.status_code, 200)
        logger.info(f"Got status {res.status_code}")
        logger.info(f"recommendations for user_id = {params['user_id']}:\n{make_report(res.json()['recs'])}")

    # для пользователя без персональных рекомендаций
    def test_non_personal(self):
        params = {"user_id": 1179649}
        logger.info(f"Testing the 10 top_popular for user_id = {params['user_id']}, with no recommendations, without a history")
        res = recommendations(params)
        self.assertEqual(res.status_code, 200)
        logger.info(f"Got status {res.status_code}")
        # logger.info(res.json()['recs'])
        logger.info(f"recommendations for user_id = {params['user_id']}:\n{make_report(res.json()['recs'])}")
