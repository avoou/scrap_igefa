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

#SELECT COUNT(*) FROM gathered_items_links_table WHERE wasScraped = 1
#SELECT COUNT(*) FROM gathered_items_info_table

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

GATHERED_ITEMS_INFO_DB = 'gathered_items_info.db'
GATHERED_ITEMS_INFO_TABLE_NAME = 'gathered_items_info_table'
GATHERED_ITEMS_INFO_COLUMNS_SCHEME = {
    "original_data_column_1": 'TEXT',
    "original_data_column_2": 'TEXT',
    "original_data_column_3": 'TEXT',
    "product_name": 'TEXT',
    "supplier_article_number": 'TEXT',
    "gtin_number": 'TEXT',
    "article_number": 'TEXT',
    "product_image_url": 'TEXT',
    "product_description": 'TEXT',
    "manufacturer": 'TEXT',
}


class NoPagesForCategory(Exception):
    pass


class CantGetJSONByURL(Exception):
    pass


async def create_db_table_if_not_exist(db_name: str, table_name: str, columns: dict):
    """Create db with db_name if there is not one"""
    async with aiosqlite.connect(db_name) as db:
        columns = ', '.join([f'{name} {column_type}' for name, column_type in columns.items()])
        await db.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
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
        gathered_categories.update(intermediate_dict)
        
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
    
    #logger.info(f'There has been gathered {count} categories') #483

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


async def write_to_db_pages_links(api_url: str, category_id: str, number_of_pages: int, links_conn):
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

    columns = ', '.join([f'{name}' for name, _ in GATHERED_ITEMS_LINKS_COLUMNS_SCHEME.items()])
    values = ', '.join([f'?' for _ in GATHERED_ITEMS_LINKS_COLUMNS_SCHEME.keys()])
    query = f"""
        INSERT INTO {GATHERED_ITEMS_LINKS_TABLE_NAME} ({columns})
        VALUES ({values})
    """
    await links_conn.executemany(query, links_rows)
    await links_conn.commit()


def make_category_url(api_url: str, category_id: str):
    return f'{api_url}/products?filter%5Btaxonomy%5D={category_id}'


def handle_count_of_pages_by_category(category_info: dict):
    try:
        pages = int((category_info["total"] / LIMIT_ITEMS_COUNT_ON_PAGE) + 1)
    except KeyError:
        raise NoPagesForCategory
    return pages if pages >= 1 else 1


async def get_json_by_url(url: str):
    pass


async def get_count_pages_by_category(category_url: str) -> int:
    async with aiohttp.ClientSession() as session:
        async with session.get(category_url) as response:
            try:
                category_info = await response.json()
            except aiohttp.client_exceptions.ContentTypeError:
                category_info = {}
        
        if category_info:
            return handle_count_of_pages_by_category(category_info)


async def get_processed_ctegories_from_db(categories: list, links_conn) -> dict:
    category_ids = ', '.join([f'"{category}"' for category in categories])
    count_of_categories_query = f"""
    SELECT category_id
    FROM {GATHERED_ITEMS_LINKS_TABLE_NAME}
    WHERE category_id IN ({category_ids})
    GROUP BY category_id
    """

    async with links_conn.execute(count_of_categories_query) as cursor:
        processed_categories = await cursor.fetchall()
    
    return {category[0]:True for category in processed_categories}


async def prepare_pages_item_links(categories: list, links_conn):
    processed_categories = await get_processed_ctegories_from_db(categories, links_conn)
    for category_id in categories:
        if category_id not in processed_categories:
            logger.info(f"Prepare links for {category_id} category_id")
            category_url = make_category_url(API_URL, category_id)
            try:
                pages = await get_count_pages_by_category(category_url)
                #print(pages, category_id, category_url)
                await write_to_db_pages_links(API_URL, category_id, pages, links_conn)
            except NoPagesForCategory:
                logger.error("Cant get count of pages for {category_url} url")
                continue
    logger.info("Links for all categories pages have been prepeared")


async def get_all_categories() -> dict:
    gathered_categories_filename = 'gathered_categories.json'
    
    if not os.path.exists(gathered_categories_filename):
        gathered_categories = await gather_all_categories(
            api_url=API_URL,
            category_id=CATEGORIES_API_ID
        )
        async with aiofiles.open(gathered_categories_filename, 'w') as f:
            await f.write(json.dumps(gathered_categories, ensure_ascii=False))

    logger.info('Using already existed file with categories')
    logger.info(f'Save list of gategories to a {gathered_categories_filename} file')
    logger.info("Getting links of items by category")
    
    async with aiofiles.open(gathered_categories_filename, 'r') as f:
        contents = await f.read()
        gathered_categories = json.loads(contents)
    #what to do if there is a file but without any category???
    return gathered_categories


async def prepare_categories(gathered_categories: dict) -> list:
    categories = [gathered_categories[category]["category_id"] for category in gathered_categories]

    logger.info(f"There are {len(categories)} categories")
    return categories


async def save_item_info_to_db(item_info: dict, info_conn):
    columns = ', '.join([f'{name}' for name, _ in GATHERED_ITEMS_INFO_COLUMNS_SCHEME.items()])
    values = ', '.join([f'?' for _ in GATHERED_ITEMS_INFO_COLUMNS_SCHEME.keys()])
    query = f"""
        INSERT INTO {GATHERED_ITEMS_INFO_TABLE_NAME} ({columns})
        VALUES ({values})
    """
    row = [tuple(item for _, item in item_info.items())]
    await info_conn.executemany(query, row)
    await info_conn.commit()


async def mark_link_as_scraped_in_db(page_link: str, category_id: str, links_conn):
    await links_conn.execute(
        f"""
        UPDATE gathered_items_links_table
        SET wasScraped = {1}
        WHERE category_id = "{category_id}"
        AND link = "{page_link}"
        """
    )
    await links_conn.commit()


async def parse_item_info(items_json: dict, page_link: str, category_id: str, info_conn, links_conn):
    if items_json.get('hits'):
        for item in items_json['hits']:
            try:
                original_data_column_3 = item.get('mainVariant', {}).get('description').split('\n---\n')[0]
            except AttributeError:
                original_data_column_3 = None
            try:
                product_description = item.get('mainVariant', {}).get('description').split('\n---\n')[1]
            except IndexError:
                product_description = None
            manufacturer = ''.join([attr.get('label') for attr in item.get('clientFields', {}).get('attributes', []) if attr.get('label', '') == "Hersteller"])
            manufacturer = None if not manufacturer else manufacturer
            product_image_url = ';'.join([image.get('url') for image in item.get('mainVariant', {}).get('images', [])])
            product_image_url = None if not product_image_url else product_image_url
    
            item_info = {
                "original_data_column_1": 'original_data_column_1',
                "original_data_column_2": item.get('variationName'),
                "original_data_column_3": original_data_column_3,
                "product_name": item.get('mainVariant', {}).get('name'),
                "supplier_article_number": item.get('mainVariant', {}).get('sku'),
                "gtin_number": item.get('mainVariant', {}).get('gtin'),
                "article_number": item.get('skuProvidedBySupplier'),
                "product_image_url": product_image_url,
                "product_description": product_description,
                "manufacturer": manufacturer
            }
    
            await save_item_info_to_db(item_info, info_conn)
            await mark_link_as_scraped_in_db(page_link, category_id, links_conn)
        #logger.info(f'Processed {page_link} url')
        

async def process_page(page_link: str, category_id: str, info_conn, links_conn):
    
    async with aiohttp.ClientSession() as session:
        async with session.get(page_link) as response:
            try:
                category_info = await response.json()
            except aiohttp.client_exceptions.ContentTypeError:
                raise CantGetJSONByURL
        
    await parse_item_info(category_info, page_link, category_id, info_conn, links_conn)


async def handle_wasnt_scraped_category(wasnt_scraped_categories: list[tuple], info_conn, links_conn):
    for category, page_link in wasnt_scraped_categories:
        try:
            await process_page(page_link, category, info_conn, links_conn)
        except CantGetJSONByURL:
            logging.error(f"Cant get JSON from {page_link} URL")


async def scraping_items_info(categories: list, info_conn, links_conn):
    for category in categories:
        logger.info(f'Scraping items info for {category} category')

        wasnt_scraped_categories_yet_query = f"""
            SELECT category_id, link
            FROM {GATHERED_ITEMS_LINKS_TABLE_NAME}
            WHERE category_id = "{category}"
            AND wasScraped = 0
        """

        async with links_conn.execute(wasnt_scraped_categories_yet_query) as cursor:
            wasnt_scraped_categories = await cursor.fetchall()
        
        await handle_wasnt_scraped_category(wasnt_scraped_categories, info_conn, links_conn)


async def main():
    logger.info("Step: 1 \nGathering all categories of items...")
    gathered_categories = await get_all_categories()

    logger.info("Step: 2 \nPreparing ctagories need to be scraped")
    categories = await prepare_categories(gathered_categories)
    
    await create_db_table_if_not_exist(
            GATHERED_ITEMS_LINKS_DB, 
            GATHERED_ITEMS_LINKS_TABLE_NAME, 
            GATHERED_ITEMS_LINKS_COLUMNS_SCHEME
        )
    await create_db_table_if_not_exist(
            GATHERED_ITEMS_INFO_DB, 
            GATHERED_ITEMS_INFO_TABLE_NAME, 
            GATHERED_ITEMS_INFO_COLUMNS_SCHEME
        )
    async with aiosqlite.connect(GATHERED_ITEMS_LINKS_DB) as db:
        links_conn = db
        await prepare_pages_item_links(categories, links_conn)

    logger.info("Step: 3 \nScraping items info for each category")
    async with aiosqlite.connect(GATHERED_ITEMS_INFO_DB) as db:
        info_conn = db
        async with aiosqlite.connect(GATHERED_ITEMS_LINKS_DB) as db:
            links_conn = db
            await scraping_items_info(categories, info_conn, links_conn)


if __name__ == '__main__':
    asyncio.run(main())
