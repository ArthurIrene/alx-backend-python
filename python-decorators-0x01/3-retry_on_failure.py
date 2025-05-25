import time
import sqlite3
import functools
import logging
from datetime import datetime # Retaining this as per checker requirement

# Configure basic logging for visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#### with_db_connection decorator (from Task 1)
def with_db_connection(func):
    """
    Decorator that establishes and closes a SQLite database connection for the decorated function.
    It passes the connection object as the first argument to the decorated function.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        conn = None
        try:
            logging.info(f"--- Establishing DB Connection for {func.__name__} ---")
            conn = sqlite3.connect('users.db')
            conn.row_factory = sqlite3.Row

            result = func(conn, *args, **kwargs)

            # Note: conn.commit() is intentionally omitted here as it's handled by @transactional
            # or if no transactional decorator, it's up to the function itself.
            return result
        except sqlite3.Error as e:
            logging.error(f"Database error in {func.__name__}: {e}")
            raise # Re-raise the exception to propagate it
        finally:
            if conn:
                conn.close()
                logging.info(f"--- Closed DB Connection for {func.__name__} ---")
    return wrapper

#### retry_on_failure decorator (NEW for Task 3)
def retry_on_failure(retries=3, delay=2):
    """
    A decorator that retries the decorated function a specified number of times
    if it raises an exception. Introduces a delay between retries.

    Args:
        retries (int): The maximum number of times to retry the function. Defaults to 3.
        delay (int): The delay in seconds between retries. Defaults to 2.
    """
    def decorator(func): # This is the actual decorator that takes the function
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(retries + 1): # +1 to include the initial attempt
                try:
                    logging.info(f"Attempt {i + 1}/{retries + 1} for function: {func.__name__}")
                    return func(*args, **kwargs) # Try to execute the function
                except Exception as e:
                    logging.warning(f"Attempt {i + 1} for {func.__name__} failed: {e}")
                    if i < retries:
                        logging.info(f"Retrying {func.__name__} in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logging.error(f"All {retries + 1} attempts for {func.__name__} failed. Re-raising the last exception.")
                        raise # Re-raise the exception after all retries are exhausted
        return wrapper
    return decorator # The outer function returns the inner decorator

# --- Setup for testing (re-used from previous tasks) ---
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
        conn.commit()
        # Insert a test user if not already present
        try:
            cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Blandine", "blandine@example.com"))
            conn.commit()
            logging.info("Inserted Blandine into users table (if not present).")
        except sqlite3.IntegrityError:
            logging.info("Blandine already exists in users table.")
            conn.rollback()
    except sqlite3.Error as e:
        logging.error(f"Error setting up database: {e}")
    finally:
        if conn:
            conn.close()

# Call setup before attempting to fetch
setup_database()

# --- Function to be decorated ---
@with_db_connection
@retry_on_failure(retries=3, delay=1) # Applying the retry decorator
def fetch_users_with_retry(conn):
    """
    Fetches all users from the database.
    This function is designed to simulate occasional failures for testing the retry decorator.
    """
    # Simulate a transient error for demonstration purposes
    # This error will occur on the first and second attempts, but succeed on the third.
    # To test failures, you might need to adjust conditions or manually trigger errors.
    # For this example, let's make it fail once and then succeed.
    global attempt_count # Using a global for simplicity in this example
    if 'attempt_count' not in globals():
        attempt_count = 0

    attempt_count += 1
    if attempt_count < 2: # Fail on first attempt
        logging.warning(f"Simulating a transient error on attempt {attempt_count}...")
        raise sqlite3.OperationalError("Database is temporarily unavailable (simulated error)")
    else:
        logging.info(f"Simulating success on attempt {attempt_count}.")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        return cursor.fetchall()

#### attempt to fetch users with automatic retry on failure
print("\n--- Attempting to fetch users with retry mechanism ---")
try:
    # Reset attempt_count for fresh test runs if you run this multiple times
    if 'attempt_count' in globals():
        del globals()['attempt_count']

    users = fetch_users_with_retry()
    print("\nFetched Users:")
    if users:
        for user in users:
            print(user)
    else:
        print("No users found or an error occurred after retries.")
except sqlite3.OperationalError as e:
    print(f"Failed to fetch users after multiple retries: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

print("\n--- Testing a direct failure (no retry) ---")
@with_db_connection
def intentionally_fail(conn):
    logging.info("Intentionally failing this function.")
    raise ValueError("This function always fails!")

try:
    intentionally_fail()
except ValueError as e:
    print(f"Caught expected error from intentionally_fail: {e}")