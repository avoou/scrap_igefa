import aiofiles
import aiohttp
import asyncio
import aiosqlite 
import json
import os
import logging
#import sqlite3

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
#https://store.igefa.de/p/clean-and-clever-pro-ultranetzender-reiniger-pro-24-pro24-ultranetzender-reiniger/XVznJhi5M3mqCyBt2XKdr3


logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


TARGET_URL = 'https://store.igefa.de'
CATEGORIES_API_ID = 'f4SXre6ovVohkGNrAvh3zR'
API_URL = 'https://api.igefa.de/shop/v1'
LIMIT_ITEMS_COUNT_ON_PAGE= 20
GATHERED_ITEMS_LINKS_DB = 'gathered_items_links.db'
GATHERED_ITEMS_LINKS_TABLE_NAME = 'gathered_items_links_table'
GATHERED_ITEMS_LINKS_COLUMNS = {
    "category_id": "INTEGER",
    "link": "TEXT",
    "total_items_count": "INTEGER",
    "gathered_items_count": "INTEGER",
    "wasScraped" : "INTEGER"
}


async def create_db_table_if_not_exist(db_name: str, table_name: str, columns: dict):
    async with aiosqlite.connect(db_name) as db:
        columns = ', '.join([f'{name} {column_type}' for name, column_type in columns.items()])
        await db.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
        await db.commit()


async def make_query_to_db(db_name: str, query: str):
    async with aiosqlite.connect(db_name) as db:
        await db.execute(query)
        await db.commit()


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
        gathered_categories.update(intermediate_dict) #async write to db?
        
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
    
    logger.info(f'There has been gathered {count} categories') #483

    return gathered_categories


def get_items_id(gathered_categories: dict) -> list:
    return (info["id"] for _, info in gathered_categories.items())


def get_products_link(api_url: str, limit: int, page: int, category_id: str) -> str:
    return f'{api_url}/products?limit={limit}&page={page}&filter%5Btaxonomy%5D={category_id}&requiresAggregations=0&track=1'


async def get_items_json(session: aiohttp.ClientSession, items_by_category_url: str, limit: int) -> tuple[dict, int]:
    async with session.get(items_by_category_url) as response:
        try:
            items = await response.json()
        except:
            items={}
        try:
            pages = round(items['total']/limit)
            if pages < 0:
                pages = 1
        except KeyError:
            pages = 0
            logger.error(f'Cant get JSON from {items_by_category_url} URL')
    
    return items, pages


def make_item_link(api_url: str, id: str, slug: str):
    return f'{api_url}/p/{slug}/{id}'


async def write_links_to_db(items: dict, category_id: str, api_url: str):
    gathered_links_rows = []
    try:
        for item in items['hits']:
            mainVariant = item['mainVariant']
            link = make_item_link(api_url, mainVariant['id'], mainVariant['slug'])
            row = (category_id, link, items['total'], 1, 0)
            gathered_links_rows.append(row)
    except KeyError:
        return
        #logger.error('Cant handle fields in items JSON')
    #print(gathered_links_rows[-1])
    async with aiosqlite.connect(GATHERED_ITEMS_LINKS_DB) as db:
        columns = ', '.join([f'{name}' for name, _ in GATHERED_ITEMS_LINKS_COLUMNS.items()])
        values = ', '.join([f'?' for _ in GATHERED_ITEMS_LINKS_COLUMNS.keys()])
        query = f"""
            INSERT INTO {GATHERED_ITEMS_LINKS_TABLE_NAME} ({columns})
            VALUES ({values})
        """
        await db.executemany(query, gathered_links_rows)
        await db.commit()
    

async def get_items_links_one_category(category_id: str, api_url: str, session: aiohttp.ClientSession, page: int) -> None:
    
    limit = LIMIT_ITEMS_COUNT_ON_PAGE
    items_by_category_url = get_products_link(api_url, limit, page, category_id)
    items, pages = await get_items_json(session, items_by_category_url, limit)
    if items:
        logger.info(f"Process {category_id} category_id ... \nThere are {pages - page} pages")
        await write_links_to_db(items, category_id, api_url)
    
    if page < pages:
        for page in range(page+1, pages+1):
            #print(items_by_category_url)
            items_by_category_url = get_products_link(api_url, limit, page, category_id)
            items, pages = await get_items_json(session, items_by_category_url, limit)
            if items:
                await write_links_to_db(items, category_id, api_url)


async def get_items_links_all_caregories(categories: dict, api_url: str) -> dict:
    
    async with aiohttp.ClientSession() as session:
        tasks = [
            get_items_links_one_category(
                category_id=category_id, 
                api_url=api_url, 
                session=session,
                page=next_page
            )
            for category_id, next_page in categories.items()
        ]
        #logger.info(f'There are {len(tasks)} tasks')
        await asyncio.gather(*tasks)

{
    "AjPEJ5AjiEqXBLVWPcjzFB": 1
}


async def prepare_categories(categories: list) -> dict:
    category_ids = ', '.join([f'"{category}"' for category in categories])
    count_of_categories_query = f"""
    SELECT category_id, total_items_count AS total_count, COUNT(*) AS link_count
    FROM {GATHERED_ITEMS_LINKS_TABLE_NAME}
    WHERE category_id IN ({category_ids})
    GROUP BY category_id
    """
    #I know only count of pages but I dont about specific numbers of pages been processed!!! its a bug
    #add column page for each link. sort by category get numbers of pages

    async with aiosqlite.connect(GATHERED_ITEMS_LINKS_DB) as db:
        async with db.execute(count_of_categories_query) as cursor:
            category_counts = await cursor.fetchall()
    
    already_scraped_categories = {}
    
    for category_id, total_count, link_count in category_counts:
        current_processed_pages = int(link_count / LIMIT_ITEMS_COUNT_ON_PAGE)
        already_scraped_categories[category_id] = current_processed_pages
    
    for category in categories:
        if category not in already_scraped_categories:
            already_scraped_categories[category] = 1
    
    return already_scraped_categories


async def main():
    logger.info("Step: 1 \nGathering all categories of items...")

    gathered_categories_filename = 'gathered_categories.json'

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
    
    async with aiofiles.open(gathered_categories_filename, 'r') as f:
        contents = await f.read()
        gathered_categories = json.loads(contents)

    logger.info("Prepare ctagories need to be scraped")

    categories = [gathered_categories[category]["id"] for category in gathered_categories]
    already_scraped_categories = await prepare_categories(categories)

    await create_db_table_if_not_exist(
        GATHERED_ITEMS_LINKS_DB, 
        GATHERED_ITEMS_LINKS_TABLE_NAME, 
        GATHERED_ITEMS_LINKS_COLUMNS
    )

    await get_items_links_all_caregories(
        categories=already_scraped_categories,
        api_url=API_URL, 
    )




if __name__ == '__main__':
    asyncio.run(main())