import asyncio
import logging
import os
import aiofiles
from httpx import AsyncClient
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlencode
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from logger import logger
from tqdm import tqdm

logging.getLogger("httpx").setLevel(logging.WARNING)


async def scrape_documents_links() -> list[str]:
    async with AsyncClient() as client:
        res = await client.get("https://www.archives.gov/research/jfk/release-2025")
        content = res.text

    soup = BeautifulSoup(content, "html.parser")
    a_tags = soup.select("table.datatable  td > a")
    links = list(
        map(lambda a_tag: "https://www.archives.gov" + a_tag.get("href"), a_tags)
    )
    return links


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(
        (
            ConnectionError,
            TimeoutError,
            IOError,
        )
    ),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def download_file(client: AsyncClient, url: str, out_dir: str):
    url = url.strip()
    file_name = os.path.basename(urlparse(url).path)
    out_path = os.path.join(out_dir, file_name)
    if os.path.exists(out_path):
        # logger.info(f"📄 file {file_name} exists, skipping...")
        return
    # logger.info(f"⬇️ downloading file {file_name} ")
    try:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            async with aiofiles.open(out_path, "wb") as file:
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    await file.write(chunk)
    except Exception as e:
        # logger.error(f"❌ Failed to download {url} after retries: {str(e)}")
        if os.path.exists(out_path):
            os.remove(out_path)
        raise


async def download_documents(links: list[str], out_dir: str):
    pbar = tqdm(total=len(links), desc="Downloading")
    batch_size = 4
    async with AsyncClient() as client:
        for i in range(0, len(links), batch_size):
            batch = links[i : i + batch_size]
            res = asyncio.gather(*[download_file(client, el, out_dir) for el in batch])
            await res
            pbar.update(len(batch))

    pbar.close()
