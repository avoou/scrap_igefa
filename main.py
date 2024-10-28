import aiofiles
import aiohttp
import asyncio
import json


#TODO:
#repair json to be pandas df with all scrap categories
#scraped | name | slug | id |
# False  | "name"| ...
# 
# gather all category links
# filter wich was not be scraped
# get all items from all pages for each category 
# update df


TARGET_URL = 'https://store.igefa.de'
CATEGORIES_API_SLUG = 'f4SXre6ovVohkGNrAvh3zR'
API_URL = 'https://api.igefa.de/shop/v1/taxonomies'


async def get_categories(api_url: str, id: str, gathered_json: dict, session: aiohttp.ClientSession, count: list) -> None:
    
    link = f"{api_url}/{id}"
    
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
                get_categories(api_url, child["id"], gathered_json, session, count)
                for child in categories_json["children"]
            ]
            await asyncio.gather(*tasks)


async def main():
    count=[0]
    gathered_json = {}

    async with aiohttp.ClientSession() as session:
        await get_categories(
            api_url=API_URL,
            id=CATEGORIES_API_SLUG,
            gathered_json=gathered_json,
            session=session,
            count=count
        )

    async with aiofiles.open('gathered_links.json', 'w') as f:
        await f.write(json.dumps(gathered_json))

    print(count) #483

if __name__ == '__main__':
    asyncio.run(main())