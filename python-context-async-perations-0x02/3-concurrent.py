import asyncio
import aiosqlite
import logging
from datetime import datetime

# --- Configure basic logging for visibility ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Asynchronous Database Setup ---
async def async_setup_database():
    """
    Sets up the SQLite database asynchronously, creating the users table with an 'age' column
    and inserting initial test data if it doesn't already exist.
    """
    logging.info("Starting async database setup...")
    conn = None
    try:
        conn = await aiosqlite.connect('users.db')
        conn.row_factory = aiosqlite.Row

        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER
            )
        ''')
        await conn.commit()

        try:
            await conn.execute("ALTER TABLE users ADD COLUMN age INTEGER")
            await conn.commit()
            logging.info("Added 'age' column to users table.")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e):
                logging.info("'age' column already exists in users table.")
            else:
                raise

        users_to_insert = [
            ("Blandine", "blandine@example.com", 30),
            ("Alice", "alice@example.com", 22),
            ("Charles", "charles@example.com", 45),
            ("Diana", "diana@example.com", 25)
        ]
        for name, email, age in users_to_insert:
            try:
                await conn.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", (name, email, age))
                await conn.commit()
                logging.info(f"Inserted user: {name} (age: {age}).")
            except aiosqlite.IntegrityError:
                logging.info(f"User '{name}' already exists.")
                await conn.rollback()
    except aiosqlite.Error as e:
        logging.error(f"Error setting up database asynchronously: {e}")
    finally:
        if conn:
            await conn.close()
    logging.info("Async database setup complete.")

# --- Asynchronous Fetch All Users ---
async def async_fetch_users():
    """
    Asynchronously fetches all users from the 'users' table.
    """
    logging.info("Starting async_fetch_users...")
    users = []
    try:
        async with aiosqlite.connect('users.db') as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute("SELECT * FROM users")
            users = await cursor.fetchall()
            # No need for cursor.close() here; async with handles connection closure.
            # Closing the cursor is implicitly handled when the connection closes.

        logging.info(f"Finished async_fetch_users: Fetched {len(users)} users.")
        return users
    except aiosqlite.Error as e:
        logging.error(f"Error in async_fetch_users: {e}")
        return []

# --- Asynchronous Fetch Users Older Than 40 ---
async def async_fetch_older_users():
    """
    Asynchronously fetches users older than 40 from the 'users' table.
    """
    logging.info("Starting async_fetch_older_users...")
    older_users = []
    try:
        async with aiosqlite.connect('users.db') as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute("SELECT * FROM users WHERE age > ?", (40,))
            older_users = await cursor.fetchall()
            # No need for cursor.close() here

        logging.info(f"Finished async_fetch_older_users: Fetched {len(older_users)} older users.")
        return older_users
    except aiosqlite.Error as e:
        logging.error(f"Error in async_fetch_older_users: {e}")
        return []

# --- Function to run queries concurrently using asyncio.gather ---
async def fetch_concurrently():
    """
    Runs async_fetch_users and async_fetch_older_users concurrently
    using asyncio.gather and prints their results.
    """
    logging.info("\n--- Starting concurrent fetches with asyncio.gather ---")
    # asyncio.gather runs the awaitables concurrently and returns their results in a list
    all_users, older_users = await asyncio.gather(
        async_fetch_users(),       # Call the async function
        async_fetch_older_users()  # Call the async function
    )
    logging.info("--- Concurrent fetches completed ---")

    print("\nAll Users:")
    if all_users:
        for user in all_users:
            print(f"  ID: {user['id']}, Name: {user['name']}, Email: {user['email']}, Age: {user['age']}")
    else:
        print("  No users found.")

    print("\nUsers Older than 40:")
    if older_users:
        for user in older_users:
            print(f"  ID: {user['id']}, Name: {user['name']}, Email: {user['email']}, Age: {user['age']}")
    else:
        print("  No users found older than 40.")

# --- Main entry point for the asyncio program ---
if __name__ == "__main__":
    print("Running asynchronous database operations...")
    # First, ensure the database is set up asynchronously
    asyncio.run(async_setup_database())
    # Then, run the concurrent fetches
    asyncio.run(fetch_concurrently())
    print("\nAsynchronous operations finished.")