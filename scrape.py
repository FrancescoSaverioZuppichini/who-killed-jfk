from httpx import AsyncClient
from bs4 import BeautifulSoup


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
