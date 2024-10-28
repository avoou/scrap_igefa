import aiohttp
import asyncio
import requests
import json

from bs4 import BeautifulSoup, Tag


TARGET_URL = 'https://store.igefa.de'
CATEGORIES_API_ID = 'f4SXre6ovVohkGNrAvh3zR'
API_URL = 'https://api.igefa.de/shop/v1/taxonomies'


def get_catiogories_from_api(api_url: str, id: str) -> json:
    #https://api.igefa.de/shop/v1/taxonomies/UZ58DPNjGf6axF3MRtAw6Q
    link = f"{api_url}/{id}"
    return requests.get(link).json()


def parse_categories_json(categories_json: dict, gathered_json: dict, count: list) -> None:

    intermediate_json = {
        categories_json["slug"]:{
            "name": categories_json["name"],
            "id": categories_json["id"],
        }
    }
    gathered_json.update(intermediate_json)
    count[0] += 1
    
    if categories_json["children"]:
        for child in categories_json["children"]:
            parse_categories_json(child, gathered_json, count)
    else:
        return



def main():
    gathered_json = {}
    count = [0]

    categories_json = get_catiogories_from_api(
        api_url=API_URL, 
        id=CATEGORIES_API_ID, 
    )

    parse_categories_json(
        categories_json=categories_json,
        gathered_json=gathered_json,
        count=count
    )

    print(count) #72 count seems similar to the one on web site

    with open('gathered_links2.json', 'w') as f:
        f.write(json.dumps(gathered_json))



if __name__ == '__main__':
    main()