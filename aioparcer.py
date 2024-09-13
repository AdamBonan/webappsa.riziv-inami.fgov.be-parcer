import asyncio
import aiohttp
from aiohttp import ClientSession
from lxml import html
import pandas as pd

import os
import argparse
from datetime import datetime
import modeles


semaphore = asyncio.Semaphore(15)


async def fetch_get(session: ClientSession, url, chunk: int = -1, back_url: bool = True, **kwargs):
    try:
        async with semaphore:
            async with session.get(url, ssl=False, **kwargs) as response:

                if response.ok:
                    content = await response.content.read(chunk)
                    if back_url:
                        return content, url
                    return content

    except aiohttp.client_exceptions.ClientPayloadError:
        pass


async def get_all_profession(urls: list[str]):
    async with ClientSession(headers=modeles.headers) as session:
        results = await asyncio.gather(*[
            fetch_get(session, url, chunk=150)
            for url in urls
        ])

        return results


async def get_enumeration() -> dict[int, int]:
    profession_urls = [modeles.url_model.replace("Profession=", f"Profession={n}")
                       for n in modeles.profesion_ids]
    results = await get_all_profession(profession_urls)

    id_and_count_page = {}
    for result in results:
        tree = html.fromstring(result[0])
        page_count = tree.get_element_by_id("div-results").get("data-page-count", 0)

        profession_id = "".join([i for i in result[1] if str.isdigit(i)])
        id_and_count_page[int(profession_id)] = int(page_count)

    return id_and_count_page


async def get_all_pages(profession_id: int, page_count: int):
    urls = [modeles.url_model.replace("Profession=", f"Profession={profession_id}").replace("PageNumber=", f"PageNumber={num}")
            for num in range(1, page_count+1)]

    async with ClientSession(headers=modeles.headers) as session:
        results = await asyncio.gather(*[
            fetch_get(session, url, back_url=False)
            for url in urls
        ])
        return results


async def main(file_name: str):
    start = datetime.now()

    enumeration = await get_enumeration()

    total_pages = sum(enumeration.values())
    count_pages = 0

    for profession in enumeration:
        pages = await get_all_pages(profession, enumeration[profession])

        data = {
            "name": [],
            "inami_number": [],
            "profession": [],
            "conv": [],
            "qualification": [],
            "date_of_qualif": [],
            "adresse": []
        }

        for page in pages:

            if not isinstance(page, bytes):
                continue

            tree = html.fromstring(page.decode('utf-8'))
            all_card = tree.xpath('//div[@class="card"]')

            for card in all_card:
                all_info = [value.replace(",", " ").strip() for value in card.text_content().split("\n") if value.strip()]

                data["name"].append(all_info[1])
                data["inami_number"].append(all_info[3])
                data["profession"].append(all_info[5])
                data["conv"].append(all_info[7])
                data["qualification"].append(all_info[9])
                data["date_of_qualif"].append(all_info[11])
                data["adresse"].append(" ".join(all_info[13:]))

        # Save to csv
        header = False
        if not os.path.exists(file_name):
            header = True
        df = pd.DataFrame(data)
        df.to_csv(f"{file_name}.csv", index=False, header=header, mode="a")

        # Counter
        count_pages += len(pages)
        print(f"Complete: {count_pages}/{total_pages} pages")

    print(f"Total time: {datetime.now() - start}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filename", default="data", help="Filename without '.csv', default = 'data'")
    args = parser.parse_args()

    asyncio.run(main(args.filename))
