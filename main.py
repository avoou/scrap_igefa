import aiofiles
import aiohttp
import asyncio
import json
import os


#TODO:
#repair json to be pandas df with all scrap categories
#scraped | name | slug | id |
# False  | "name"| ...
# 
# gather all category links
# filter wich was not be scraped
# get all items from all pages for each category 
# update df

#https://api.igefa.de/shop/v1/products/by-variant/XVznJhi5M3mqCyBt2XKdr3
#https://api.igefa.de/shop/v1/products?filter%5Btaxonomy%5D=UZ58DPNjGf6axF3MRtAw6Q
#https://api.igefa.de/shop/v1/products?limit=20&page=1&filter%5Btaxonomy%5D=axwgQ9VwrwBYWYLCkp2oKj&requiresAggregations=0&track=1

TARGET_URL = 'https://store.igefa.de'
CATEGORIES_API_ID = 'f4SXre6ovVohkGNrAvh3zR'
API_URL = 'https://api.igefa.de/shop/v1/taxonomies'


async def get_category(api_url: str, category_id: str, gathered_json: dict, session: aiohttp.ClientSession, count: list) -> None:
    
    link = f"{api_url}/{category_id}"
    
    async with session.get(link) as response:
        count[0] += 1
        categories_json = await response.json()
        try:
            name = categories_json["name"].encode('latin1').decode('utf-8')
        except UnicodeDecodeError:
            name = categories_json["name"]
        intermediate_json = {
            categories_json["slug"]: {
                "scraped": False,
                "name": name,
                "id": categories_json["id"],
            }
        }
        gathered_json.update(intermediate_json)
        
        if categories_json.get("children"):
            tasks = [
                get_category(api_url, child["id"], gathered_json, session, count)
                for child in categories_json["children"]
            ]
            await asyncio.gather(*tasks)


async def gather_all_categories(api_url: str, category_id: str) -> dict:
    count=[0]
    gathered_json = {}

    async with aiohttp.ClientSession() as session:
        await get_category(
            api_url=api_url,
            category_id=category_id,
            gathered_json=gathered_json,
            session=session,
            count=count
        )
    
    print(f'There has been gathered {count} categories') #483

    return gathered_json


def make_links(gathered_json: dict):
    
    for category, info in gathered_json.items():
        category_id = info["id"]
        items_by_category_url = f'https://api.igefa.de/shop/v1/products?filter%5Btaxonomy%5D={category_id}'


async def main():
    print("Step: 1 \nGathering all categories of items...")

    output_filename = 'gathered_links.json'

    if not os.path.exists(output_filename):
        gathered_json = await gather_all_categories(
            api_url=API_URL,
            category_id=CATEGORIES_API_ID
        )
        async with aiofiles.open(output_filename, 'w') as f:
            await f.write(json.dumps(gathered_json, ensure_ascii=False))
    print(f'Step: 2 \nSave list of gategories to a {output_filename} file')
    
    print("Step: 3 \nGetting links of items by category")
    
    with open(output_filename, 'r') as f:
        gathered_json = json.load(f)


    

if __name__ == '__main__':
    asyncio.run(main())