import requests 
import pprint 
import csv 
import concurrent.futures 
from datetime import datetime 

STORE_ID = "27"

PRODUCTS_URL = "https://shop.foodbazaar.com/api/v2/store_products"
USERS_URL = "https://shop.foodbazaar.com/api/v2/user"
INIT_USER_URL = 'https://shop.foodbazaar.com/api/v3/user_init?with_configs=true'

def _get_categories(store_id):
    res = requests.get( f"https://shop.foodbazaar.com/api/v2/categories/store/{store_id}")
    return res.json()['items']

def _get_items(category_id, cookie, additional_params={}):
    all_items = []
    offset = 0
    while True:
        params={'offset': offset, **additional_params}
        if category_id is not None:
            params['category_id'] = category_id

        res = requests.get(PRODUCTS_URL,
            params=params,
            headers = {'Cookie': cookie},
        )
        res_json = res.json()
        items = res_json['items']
        all_items+=items
        if len(items) < 100:
            break 
        offset += 100
    return all_items


def _get_all_keys(dicts):
    all_keys = set()
    for dictionary in dicts:
        all_keys = all_keys | dictionary.keys()
    return all_keys

def get_inventory(store_id):
    start_dt = datetime.now()
    cookie = _get_cookie()

    store = _set_store(store_id, cookie)
    print(f'Store: {store["store"]["name"]}')
    categories = _get_categories(store_id)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        args = [(
            category['id'],
            cookie
        ) for category in categories]
        item_lists = executor.map(_get_items, *zip(*args))
    
    all_items = [item for items in item_lists for item in items]
    items_dict = {}
    for item in all_items:
        items_dict[item['id']] = item
    
    unique_items = items_dict.values()
    print(f'Found {len(unique_items)} items - it took {datetime.now() - start_dt}!')

    fieldnames = _get_all_keys(unique_items)
    with open('items.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in unique_items:
            writer.writerow(item)


def _set_store(store_id, cookie):
    res = requests.patch(USERS_URL, json={"store_id": str(store_id),"has_changed_store": True}, headers = {'Cookie': cookie})
    res.raise_for_status()
    return res.json()

def _get_cookie():
    res = requests.post(INIT_USER_URL,
        json={"binary":"web-ecom","binary_version":"4.33.27","is_retina":False,"os_version":"MacIntel",
        "pixel_density":"2.0","push_token":"","screen_height":900,"screen_width":1440}
)
    res.raise_for_status()
    return f"session-prd-fbz={res.cookies['session-prd-fbz']}"


if __name__ == '__main__':
    get_inventory(STORE_ID)
    