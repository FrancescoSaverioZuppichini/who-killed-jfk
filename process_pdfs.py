import os
import aiofiles
import aiosqlite
from dotenv import load_dotenv
from os import environ
from pathlib import Path
from crud import (
    Document,
    Page,
    add_documents_from_links,
    add_pages,
    get_db,
    get_document_pages,
    get_documents,
)
from logger import logger
import asyncio
from process import convert_pdf_to_images

import multiprocessing as mp
from tqdm import tqdm

load_dotenv()

SQLITE_PATH = environ["SQLITE_PATH"]
FILES_DIR = environ["FILES_DIR"]
IMAGES_DIR = environ["IMAGES_DIR"]
LINKS_TEXT_PATH = environ["LINKS_TEXT_PATH"]

files_dir = Path(IMAGES_DIR)
files_dir.mkdir(exist_ok=True, parents=True)


def process_one_document_wrapper(document: Document):
    return asyncio.run(process_one_document(document))


async def process_one_document(document: Document):
    db = await get_db(SQLITE_PATH)
    pdf_path = os.path.join(FILES_DIR, f"{document["name"]}")
    images_names = convert_pdf_to_images(pdf_path, IMAGES_DIR)
    if await get_document_pages(db, document_id=document["id"]):
        return
    pages: list[Page] = [
        {"name": name, "document_id": document["id"], "page": idx}
        for idx, name in enumerate(images_names)
    ]
    await add_pages(db, pages)

    await db.close()
    return


async def main():
    db = await get_db(SQLITE_PATH)
    documents = await get_documents(db)

    num_cpus = mp.cpu_count()
    logger.info(f" Ready to process {len(documents)} documents with {num_cpus} cpus")
    with mp.Pool(processes=num_cpus) as pool:
        results = list(
            tqdm(
                pool.imap(
                    process_one_document_wrapper,
                    [dict(document) for document in documents],
                ),
                total=len(documents),
                desc="Processing documents",
            )
        )

    logger.info(f"✅ Processed {len(results)} documents and added to the database")
    return


if __name__ == "__main__":
    asyncio.run(main())
