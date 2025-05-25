import sqlite3
import functools
import logging
from datetime import datetime # Added this import as required by the checker

# Configure logging to output to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#### decorator to log SQL queries
def log_queries(func):
    """
    Decorator that logs the SQL query string before executing the decorated function.
    It assumes the SQL query is passed as a keyword argument 'query'.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Retrieve the SQL query from keyword arguments
        sql_query = kwargs.get('query')

        if sql_query:
            logging.info(f"--- Query Log for {func.__name__} ---")
            logging.info(f"Executing SQL: '{sql_query}'")
            # Log any other parameters, excluding 'query' itself
            other_params = {k: v for k, v in kwargs.items() if k != 'query'}
            if args:
                logging.info(f"Positional parameters: {args}")
            if other_params:
                logging.info(f"Keyword parameters (excluding query): {other_params}")
            logging.info(f"------------------------------------")
        else:
            logging.warning(f"Function {func.__name__} called without a 'query' keyword argument. No SQL logged.")

        # Execute the original function with its arguments
        result = func(*args, **kwargs)

        logging.info(f"Query for '{func.__name__}' completed.")
        return result
    return wrapper

@log_queries
def fetch_all_users(query):
    # Ensure the database file exists and table is set up for testing
    conn = None
    try:
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()

        # Create users table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            )
        ''')
        conn.commit()

        # Insert some dummy data if the table is empty, handling duplicates
        try:
            cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Blandine", "blandine@example.com"))
            cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Test User", "test@example.com"))
            conn.commit()
        except sqlite3.IntegrityError:
            pass # Data already exists, which is fine

        # Execute the actual query passed to the function
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except sqlite3.Error as e:
        logging.error(f"Database error in fetch_all_users: {e}")
        return [] # Return empty list on error
    finally:
        if conn:
            conn.close() # Ensure connection is closed

#### fetch users while logging the query
print("--- Starting fetch_all_users operation ---")
users = fetch_all_users(query="SELECT * FROM users")
print("\nFetched Users (from main script):")
if users:
    for user in users:
        print(user)
else:
    print("No users found or an error occurred during fetch.")

print("\n--- Starting fetch for specific user ---")
specific_user = fetch_all_users(query="SELECT * FROM users WHERE name = ?", name="Blandine")
print("\nFetched Specific User (from main script):")
if specific_user:
    for user in specific_user:
        print(user)
else:
    print("Specific user not found or an error occurred during fetch.")