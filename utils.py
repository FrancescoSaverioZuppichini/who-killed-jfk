import aiofiles
import aiosqlite


async def create_csv(out_path: str, db: aiosqlite.Connection):
    cursor = await db.cursor()
    await cursor.execute("SELECT * from documents")
    rows = await cursor.fetchall()

    csv_header = "id,name,href"
    csv_rows = list(map(lambda row: f"{row['id']},{row['name']},{row['href']}", rows))

    async with aiofiles.open(out_path, "w") as f:
        await f.write(csv_header + "\n")
        await f.writelines(csv_rows)
