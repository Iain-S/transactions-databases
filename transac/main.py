import asyncio

import databases
import uvicorn
from asyncpg.exceptions import LockNotAvailableError
from fastapi import FastAPI
from sqlalchemy.sql import text

app = FastAPI()

# Should match that in alembic.ini
CONNECTION_DSN = "postgresql://postgres:password@localhost/postgres"
database = databases.Database(CONNECTION_DSN)

# 4s is plenty of time to manually start two requests
SLEEP_FOR = 4


@app.on_event("startup")
async def startup() -> None:
    """Called by FastAPI on startup."""
    await database.connect()


@app.on_event("shutdown")
async def shutdown() -> None:
    """Called by FastAPI on shutdown."""
    await database.disconnect()


@app.get("/")
async def read_root():
    """Demonstrate the issue we are trying to solve.

    Will run fine if called synchronously but will error if called concurrently.
    """
    results = await database.execute(text("select count(*) from test_table"))
    await asyncio.sleep(SLEEP_FOR)
    await database.execute(
        text("insert into test_table values ({}, {})".format(results + 1, 0))
    )
    return {"hello": str(results)}


@app.get("/with-transaction")
async def with_transaction():
    """Do an insert within a transaction."""
    async with database.connection() as connection:
        async with connection.transaction():
            try:
                results = await database.fetch_all(
                    text("select id from test_table for update nowait")
                )

                # It's fine to SELECT ... NOWAIT more than once in the same transaction
                await database.fetch_all(
                    text("select id from test_table for update nowait")
                )

                await asyncio.sleep(SLEEP_FOR)
                max_id = max([x["id"] for x in results])
                await database.execute(
                    text("insert into test_table values ({}, {})".format(max_id + 1, 0))
                )
            except LockNotAvailableError:
                return "Table busy"

    return {"hello": str(max_id)}


@app.get("/show-table")
async def show_table():
    """Show all the rows in test_table."""
    results = [
        {**x} for x in await database.fetch_all(text("select * from test_table"))
    ]
    return results


async def the_task(db):
    """Selects and sleeps."""
    async with db.transaction():
        await db.fetch_one("SELECT 1")
        await asyncio.sleep(1)
        await db.fetch_one("SELECT 1")


@app.get("/deadlock")
async def deadlock():
    """This will deadlock.

    The child tasks will inherit the connection created by the parent SELECT.
    Taken from https://github.com/encode/databases/issues/327"""

    # Doesn't deadlock if we remove this
    await database.fetch_one("SELECT 1")

    tasks = [the_task(database) for _ in range(2)]
    await asyncio.gather(*tasks)


async def the_other_task(db):
    """As per the_task but with a shorter wait."""
    async with db.transaction():
        await db.fetch_one("SELECT 1")
        await asyncio.sleep(0.5)
        await db.fetch_one("SELECT 1")


@app.get("/deadlock2")
async def deadlock2():
    """This will not deadlock.

    The child tasks will have different connections, presumably.
    """

    tasks = [the_other_task(database), the_task(database)]
    await asyncio.gather(*tasks)


@app.get("/nested")
async def nested():
    """Doesn't deadlock.

    We don't run multiple tasks concurrently,
    we only check that we can nest transactions."""
    async with database.transaction():
        await database.fetch_one("SELECT 1")
        # Note that we aren't gathering any tasks here
        await the_task(database)
        await database.fetch_one("SELECT 1")


async def connection_task(db):
    async with db.connection() as connection:
        async with connection.transaction():
            await connection.fetch_one("SELECT 1")
            await asyncio.sleep(1)
            await connection.fetch_one("SELECT 1")


@app.get("/connections")
async def connections():
    """This will deadlock.

    The child tasks will inherit the connection created by the parent SELECT."""

    async with database.connection() as connection:
        await connection.fetch_one("SELECT 1")

        tasks = [connection_task(database) for _ in range(4)]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
