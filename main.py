import aiofiles
import aiohttp
import asyncio
import json
import os
import logging

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
#https://api.igefa.de/shop/v1/products?limit=20&page=1&filter%5Btaxonomy%5D=UZ58DPNjGf6axF3MRtAw6Q&requiresAggregations=0&track=1


logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


TARGET_URL = 'https://store.igefa.de'
CATEGORIES_API_ID = 'f4SXre6ovVohkGNrAvh3zR'
API_URL = 'https://api.igefa.de/shop/v1'
LIMIT_ITEMS_COUNT_ON_PAGE= 20


async def get_category(api_url: str, category_id: str, gathered_categories: dict, session: aiohttp.ClientSession, count: list) -> None:
    
    link = f"{api_url}/taxonomies/{category_id}"
    
    async with session.get(link) as response:
        count[0] += 1
        categories_json = await response.json()
        try:
            name = categories_json["name"].encode('latin1').decode('utf-8')
        except UnicodeDecodeError:
            name = categories_json["name"]
        intermediate_dict = {
            categories_json["slug"]: {
                "category_name": name,
                "category_id": categories_json["id"],
            }
        }
        gathered_categories.update(intermediate_dict)
        
        if categories_json.get("children"):
            tasks = [
                get_category(api_url, child["id"], gathered_categories, session, count)
                for child in categories_json["children"]
            ]
            await asyncio.gather(*tasks)


async def gather_all_categories(api_url: str, category_id: str) -> dict:
    count=[0]
    gathered_categories = {}

    async with aiohttp.ClientSession() as session:
        await get_category(
            api_url=api_url,
            category_id=category_id,
            gathered_categories=gathered_categories,
            session=session,
            count=count
        )
    
    print(f'There has been gathered {count} categories') #483

    return gathered_categories


def get_items_id(gathered_categories: dict) -> list:
    return (info["id"] for _, info in gathered_categories.items())


def get_products_link(api_url: str, limit: int, page: int, category_id: str) -> str:
    return f'{api_url}/products?limit={limit}&page={page}&filter%5Btaxonomy%5D={category_id}&requiresAggregations=0&track=1'


async def get_items_links_one_category(gathered_links: dict, category_id: str, api_url: str, session: aiohttp.ClientSession, page, count) -> None:
    next_page = page + 1
    limit = 20
    items_by_category_url = get_products_link(api_url, limit, next_page, category_id)
    async with session.get(items_by_category_url) as response:
        
        if response.headers['Content-Type'] == 'text/plain':
            items = await response.json(content_type='text/plain')
        items = await response.json()
        try:
            pages = round(items['total']/limit)
            if page <= pages:
                intermediate_dict = {
                    category_id: []
                }
                for item in items['hits']:
                    count[0] += 1
                    item = item['mainVariant']
                    intermediate_dict[category_id].append(
                        {
                            "item_id": item['id'],
                            "item_slug": item['slug'],
                            "wasScraped": False,
                        }
                    )
                gathered_links.update(intermediate_dict)
            
                await get_items_links_one_category(gathered_links, category_id, api_url, session, next_page, count)
        except Exception:
            logger.error(f'URL: {items_by_category_url}, doesn`t work')
            #put bad link to different field in gathered_links dict
            gathered_links['bad_links'].append(items_by_category_url)

async def get_items_links_all_caregories(api_url: str, gathered_categories: dict, limit: int, page: int) -> dict:
    count=[0]
    gathered_links = {"bad_links": []}
    async with aiohttp.ClientSession() as session:
        tasks = [
            get_items_links_one_category(
                gathered_links=gathered_links,
                category_id=category_id, 
                api_url=api_url, 
                session=session, 
                page=page, 
                count=count,
            )
            for category_id in get_items_id(gathered_categories)
        ]
        await asyncio.gather(*tasks)

    logger.info(f'There has been gotten {count} links by all categories')
    return gathered_links
         



async def main():
    logger.info("Step: 1 \nGathering all categories of items...")

    gathered_categories_filename = 'gathered_categories.json'
    gathered_links_filename = 'gathered_links.json'

    if not os.path.exists(gathered_categories_filename):
        gathered_categories = await gather_all_categories(
            api_url=API_URL,
            category_id=CATEGORIES_API_ID
        )
        async with aiofiles.open(gathered_categories_filename, 'w') as f:
            await f.write(json.dumps(gathered_categories, ensure_ascii=False))

    logger.info('Using already existed file with categories')

    logger.info(f'Step: 2 \nSave list of gategories to a {gathered_categories_filename} file')
    
    logger.info("Step: 3 \nGetting links of items by category")
    
    with open(gathered_categories_filename, 'r') as f:
        gathered_categories = json.load(f)

    gathered_links = await get_items_links_all_caregories(
        api_url=API_URL, 
        gathered_categories=gathered_categories, 
        limit=LIMIT_ITEMS_COUNT_ON_PAGE,
        page=0
    )

    async with aiofiles.open(gathered_links_filename, 'w') as f:
            await f.write(json.dumps(gathered_links, ensure_ascii=False))
    logger.info(f'Step: 3 \nSave list of gategories to a {gathered_links_filename} file')




if __name__ == '__main__':
    asyncio.run(main())