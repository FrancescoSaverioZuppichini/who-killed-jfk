import os
from urllib.parse import urlparse
import aiosqlite
from typing import Optional, TypedDict
from logger import logger
import sqlite3

_connection: Optional[aiosqlite.Connection] = None


# schemas
class Document(TypedDict):
    id: int
    name: str
    href: str


class Page(TypedDict):
    id: int
    document_id: str
    page: int
    name: str


async def get_db(db_path: str) -> aiosqlite.Connection:
    global _connection
    if _connection and not _connection.close:
        return _connection
    _connection = await aiosqlite.connect(db_path)
    await _connection.execute("PRAGMA foreign_keys = ON")
    # This shit returns rows in a dictionary instead of shitty tuples
    _connection.row_factory = aiosqlite.Row
    await init(_connection)
    return _connection


async def init(db: aiosqlite.Connection):
    await db.execute(
        """
    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        href TEXT NOT NULL
    )
    """
    )
    await db.execute(
        """
    CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        document_id INTEGER NOT NULL,
        page INTEGER NOT NULL,
        name TEXT NOT NULL UNIQUE,
        FOREIGN KEY (document_id) REFERENCES documents(id)
    )
    """
    )
    await db.execute(
        """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_name ON documents(name)
    """
    )
    await db.execute(
        """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_pages_document_page ON pages(document_id, page)
    """
    )
    await db.commit()


async def add_documents_from_links(db: aiosqlite.Connection, links: list[str]):
    # let's extract the name for each link
    names = list(map(lambda link: os.path.basename(urlparse(link).path).strip(), links))
    values = list(zip(names, links))

    try:
        await db.executemany("INSERT INTO documents (name, href) VALUES (?, ?)", values)
        await db.commit()
    except sqlite3.IntegrityError as e:
        logger.warning(
            f"️💾 Some documents already exist in the database, clean the db and retry: {e}"
        )


async def get_documents(db: aiosqlite.Connection) -> aiosqlite.Row:
    cursor = await db.execute("SELECT * from documents")
    rows = await cursor.fetchall()
    return rows


async def get_document_pages(
    db: aiosqlite.Connection, document_id: int
) -> aiosqlite.Row:
    cursor = await db.execute(
        """
    SELECT * from pages 
    WHERE document_id = ?
    """,
        (document_id,),
    )
    rows = await cursor.fetchall()
    return rows


async def add_page(db: aiosqlite.Connection, page: Page):
    try:
        await db.execute(
            "INSERT INTO pages (document_id, page, name) VALUES (?, ?, ?)",
            (page["document_id"], page["page"], page["name"]),
        )
        await db.commit()
    except sqlite3.IntegrityError as e:
        logger.warning(f"💾 Page already exists in the database: {e}")


async def add_pages(db: aiosqlite.Connection, pages: list[Page]):
    try:
        async with db.execute("BEGIN TRANSACTION"):
            values = [
                (page["document_id"], page["page"], page["name"]) for page in pages
            ]
            await db.executemany(
                "INSERT INTO pages (document_id, page, name) VALUES (?, ?, ?)", values
            )

        await db.commit()

    except sqlite3.IntegrityError as e:
        await db.rollback()
        logger.warning(f"💾 Error adding pages to database: {e}")

        logger.info("💾 Falling back to adding pages individually")
        for page in pages:
            try:
                await add_page(db, page)
            except Exception as e:
                logger.error(f"💾 Failed to add page {page['name']}: {e}")
