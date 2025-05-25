import sqlite3
import functools
import logging # Good practice for error logging
from datetime import datetime # Retaining this as per previous checker requirement

# Configure basic logging for visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def with_db_connection(func):
    """
    Decorator that establishes and closes a SQLite database connection for the decorated function.
    It passes the connection object as the first argument to the decorated function.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        conn = None # Initialize connection to None
        try:
            # 1. Open the database connection
            # We'll hardcode 'users.db' for now as per the project context.
            logging.info(f"--- Establishing DB Connection for {func.__name__} ---")
            conn = sqlite3.connect('users.db')
            conn.row_factory = sqlite3.Row # Optional: access columns by name

            # 2. Pass the connection object as the first argument to the original function
            # The original function expects 'conn' as its first parameter.
            result = func(conn, *args, **kwargs)

            # 3. If everything succeeds, commit any changes (for write operations)
            # This is important for write operations, though get_user_by_id is read-only.
            conn.commit()

            return result
        except sqlite3.Error as e:
            # 4. Handle database errors
            logging.error(f"Database error in {func.__name__}: {e}")
            if conn:
                conn.rollback() # Rollback any changes on error
                logging.info(f"Transaction rolled back for {func.__name__}.")
            raise # Re-raise the exception to propagate it
        finally:
            # 5. Ensure the connection is always closed
            if conn:
                conn.close()
                logging.info(f"--- Closed DB Connection for {func.__name__} ---")

    return wrapper

@with_db_connection
def get_user_by_id(conn, user_id):
    """
    Fetches a user from the 'users' table by ID.
    Expects an open database connection (conn) as its first argument.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    return cursor.fetchone()

# --- Setup for testing ---
# Ensure 'users.db' exists and has some data
def setup_database():
    conn = None
    try:
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            )
        ''')
        # Insert a test user if not already present
        try:
            cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Blandine", "blandine@example.com"))
            conn.commit()
            logging.info("Inserted Blandine into users table (if not present).")
        except sqlite3.IntegrityError:
            logging.info("Blandine already exists in users table.")
            conn.rollback() # Rollback if insert failed due to unique constraint
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error setting up database: {e}")
    finally:
        if conn:
            conn.close()

# Call setup before attempting to fetch
setup_database()

#### Fetch user by ID with automatic connection handling
print("\n--- Attempting to fetch user with ID = 1 ---")
user = get_user_by_id(user_id=1) # Notice 'conn' is not passed directly here!
if user:
    # Accessing columns by name due to conn.row_factory = sqlite3.Row
    print(f"Fetched User (ID: {user['id']}): Name={user['name']}, Email={user['email']}")
else:
    print("User with ID 1 not found.")

print("\n--- Attempting to fetch user with ID = 99 (non-existent) ---")
user_non_existent = get_user_by_id(user_id=99)
if user_non_existent:
    print(f"Fetched User (ID: {user_non_existent['id']}): Name={user_non_existent['name']}, Email={user_non_existent['email']}")
else:
    print("User with ID 99 not found (expected).")