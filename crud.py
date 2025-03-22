import os
from urllib.parse import urlparse
import aiosqlite
from typing import Optional, TypedDict
from logger import logger
import sqlite3

_connection: Optional[aiosqlite.Connection] = None


async def get_db(db_path: str) -> aiosqlite.Connection:
    global _connection
    if _connection and not _connection.close:
        return _connection
    logger.info(f"💾 init db @{db_path} ...")
    _connection = await aiosqlite.connect(db_path)
    await _connection.execute("PRAGMA foreign_keys = ON")
    # This shit returns rows in a dictionary instead of shitty tuples
    _connection.row_factory = aiosqlite.Row
    await init(_connection)
    logger.info(f"✅ done!")
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
    CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_name ON documents(name)
    """
    )
    await db.commit()


async def add_documents_from_links(db: aiosqlite.Connection, links: list[str]):
    # let's extract the name for each link
    names = list(map(lambda link: os.path.basename(urlparse(link).path), links))
    values = list(zip(names, links))

    try:
        await db.executemany("INSERT INTO documents (name, href) VALUES (?, ?)", values)
        await db.commit()
    except sqlite3.IntegrityError as e:
        logger.warning(
            f"️💾 Some documents already exist in the database, clean the db and retry: {e}"
        )


# schemas
class Document(TypedDict):
    id: int
    name: str
    href: str
