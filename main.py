import aiofiles
import aiohttp
import asyncio
import aiosqlite 
import json
import os
import logging


#https://api.igefa.de/shop/v1/products/by-variant/XVznJhi5M3mqCyBt2XKdr3
#https://api.igefa.de/shop/v1/products?filter%5Btaxonomy%5D=AjPEJ5AjiEqXBLVWPcjzFB
#https://api.igefa.de/shop/v1/products?limit=20&page=1&filter%5Btaxonomy%5D=UZ58DPNjGf6axF3MRtAw6Q&requiresAggregations=0&track=1

#https://api.igefa.de/shop/v1/products?filter%5Bid%5D%5B0%5D=Nk8vRrg6kk6cQQFMCxTbg5&page=1&requiresAggregations=0

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


TARGET_URL = 'https://store.igefa.de'
CATEGORIES_API_ID = 'f4SXre6ovVohkGNrAvh3zR'
API_URL = 'https://api.igefa.de/shop/v1'
LIMIT_ITEMS_COUNT_ON_PAGE= 20
GATHERED_ITEMS_LINKS_DB = 'gathered_items_links.db'
GATHERED_ITEMS_LINKS_TABLE_NAME = 'gathered_items_links_table'
GATHERED_ITEMS_LINKS_COLUMNS_SCHEME = {
    "category_id": "INTEGER",
    "link": "TEXT",
    "total_items_count": "INTEGER",
    "gathered_items_count": "INTEGER",
    "wasScraped" : "INTEGER"
}


class NoPagesForCategory(Exception):
    pass


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
            for child in categories_json["children"]:
                await get_category(api_url, child["id"], gathered_categories, session, count)


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
    #https://api.igefa.de/shop/v1/products/by-variant/XVznJhi5M3mqCyBt2XKdr3
    return f'{api_url}/products/by-variant/{id}'


async def write_to_db_pages_links(api_url: str, category_id: str, number_of_pages: int):
    links_rows = [
        (
            category_id,
            get_products_link(api_url, LIMIT_ITEMS_COUNT_ON_PAGE, page, category_id),
            number_of_pages*20,
            1,
            0

        )
        for page in range(1, number_of_pages+1)
    ]

    async with aiosqlite.connect(GATHERED_ITEMS_LINKS_DB) as db:
        columns = ', '.join([f'{name}' for name, _ in GATHERED_ITEMS_LINKS_COLUMNS_SCHEME.items()])
        values = ', '.join([f'?' for _ in GATHERED_ITEMS_LINKS_COLUMNS_SCHEME.keys()])
        query = f"""
            INSERT INTO {GATHERED_ITEMS_LINKS_TABLE_NAME} ({columns})
            VALUES ({values})
        """
        await db.executemany(query, links_rows)
        await db.commit()


def make_category_url(api_url: str, category_id: str):
    return f'{api_url}/products?filter%5Btaxonomy%5D={category_id}'


def handle_count_of_pages_by_category(category_info: dict):
    try:
        pages = int((category_info["total"] / LIMIT_ITEMS_COUNT_ON_PAGE) + 1)
    except KeyError:
        raise NoPagesForCategory
    return pages if pages >= 1 else 1


async def get_count_pages_by_category(category_url: str) -> int:
    async with aiohttp.ClientSession() as session:
        async with session.get(category_url) as response:
            try:
                category_info = await response.json()
            except aiohttp.client_exceptions.ContentTypeError:
                category_info = {}
        
        if category_info:
            return handle_count_of_pages_by_category(category_info)


async def get_processed_ctegories_from_db(categories: list) -> dict:
    category_ids = ', '.join([f'"{category}"' for category in categories])
    count_of_categories_query = f"""
    SELECT category_id
    FROM {GATHERED_ITEMS_LINKS_TABLE_NAME}
    WHERE category_id IN ({category_ids})
    GROUP BY category_id
    """
    #I know only count of pages but I dont about specific numbers of pages been processed!!! its a bug
    #add column page for each link. sort by category get numbers of pages

    async with aiosqlite.connect(GATHERED_ITEMS_LINKS_DB) as db:
        async with db.execute(count_of_categories_query) as cursor:
            processed_categories = await cursor.fetchall()
    
    return {category[0]:True for category in processed_categories}


async def prepare_pages_item_links(categories: list):
    processed_categories = await get_processed_ctegories_from_db(categories)
    for category_id in categories:
        if category_id not in processed_categories:
            logger.info(f"Prepare links for {category_id} category_id")
            category_url = make_category_url(API_URL, category_id)
            try:
                pages = await get_count_pages_by_category(category_url)
                #print(pages, category_id, category_url)
                await write_to_db_pages_links(API_URL, category_id, pages)
            except NoPagesForCategory:
                logger.error("Cant get count of pages for {category_url} url")
                continue


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

    logger.info("Preparing ctagories need to be scraped")
    await create_db_table_if_not_exist(
            GATHERED_ITEMS_LINKS_DB, 
            GATHERED_ITEMS_LINKS_TABLE_NAME, 
            GATHERED_ITEMS_LINKS_COLUMNS_SCHEME
        )

    categories = [gathered_categories[category]["category_id"] for category in gathered_categories]
    logger.info(f"There are {len(categories)} categories")

    await prepare_pages_item_links(categories)




if __name__ == '__main__':
    asyncio.run(main())