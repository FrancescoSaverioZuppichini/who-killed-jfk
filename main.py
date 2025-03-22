import aiofiles
from dotenv import load_dotenv
from os import environ
from pathlib import Path
from crud import add_documents_from_links, get_db
from logger import logger
from scrape import scrape_documents_links
import asyncio

from utils import create_csv

load_dotenv()

SQLITE_PATH = environ["SQLITE_PATH"]
FILES_DIR = environ["FILES_DIR"]
LINKS_TEXT_PATH = environ["LINKS_TEXT_PATH"]

files_dir = Path(FILES_DIR)
files_dir.mkdir(exist_ok=True, parents=True)


async def main():
    if Path(LINKS_TEXT_PATH).exists():
        logger.info("📦 links file found, loading ...")
        async with aiofiles.open(LINKS_TEXT_PATH, mode="r") as f:
            links = await f.readlines()
    else:
        links = await scrape_documents_links()
        async with aiofiles.open(LINKS_TEXT_PATH, mode="w") as f:
            await f.write("\n".join(links))

    logger.info(f"🤖 scraped {len(links)} links.")
    db = await get_db(SQLITE_PATH)
    await add_documents_from_links(db, links)
    logger.info(f"✅ added to the database")
    await create_csv("data.csv", db)
    logger.info(f"✅ created .csv")
    await db.close()
    return


if __name__ == "__main__":
    asyncio.run(main())
