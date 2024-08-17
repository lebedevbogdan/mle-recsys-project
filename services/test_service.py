import requests
import unittest

recommendations_url = 'http://127.0.0.1:8000'
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

def recommendations(params:dict) -> dict:
    resp = requests.post(recommendations_url+'/recommendations', headers=headers, params=params)
    if resp.status_code == 200:
        recs = resp.json()
        return recs
    else:
        print(f"status code: {resp.status_code}")
    
class RecommenderTestCase(unittest.TestCase):
    # для пользователя с персональными рекомендациями, но без онлайн-истории
    def test__only_personal(self):
        params = {"user_id": 1}
        res = recommendations(params)
        res = res['recs']
        awaited_res = [37615, 38318, 65011, 33627, 105322, 52100, 21519270, 12480, 10177, 22771]
        self.assertEqual(res, awaited_res)

    # для пользователя с персональными рекомендациями и онлайн-историей
    def test_personal_online(self):
        params = {"user_id": 0}
        res = recommendations(params)
        res = res['recs']
        awaited_res = [582507, 41280596, 1177, 37849072, 1123, 57947608, 33627, 36951891, 34608, 416032]
        self.assertEqual(res, awaited_res)

    # для пользователя без персональных рекомендаций
    def test_non_personal(self):
        params = {"user_id": 1179649}
        res = recommendations(params)
        res = res['recs']
        awaited_res = [53404, 33311009, 178529, 35505245, 24692821, 795836, 6705392, 32947997, 37384, 45499814]
        self.assertEqual(res, awaited_res)
