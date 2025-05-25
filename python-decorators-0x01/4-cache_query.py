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

# Global cache dictionary for storing query results
# As specified by the task.
query_cache = {}

#### cache_query decorator (NEW for Task 4)
def cache_query(func):
    """
    Decorator that caches the results of database queries.
    The cache key is generated from the function's arguments (specifically the 'query' keyword argument).
    Assumes the SQL query string is passed as a keyword argument named 'query'.
    """
    @functools.wraps(func)
    def wrapper(conn, *args, **kwargs): # conn is passed by @with_db_connection
        # The cache key should uniquely identify the query and its parameters
        # For simplicity, we'll use the 'query' argument and other relevant kwargs as a key.
        # It's important that the key is hashable (e.g., a tuple).

        # Extract the query string from kwargs
        sql_query = kwargs.get('query')
        if not sql_query:
            logging.warning(f"Function {func.__name__} decorated with @cache_query but no 'query' keyword argument found. Caching will not apply.")
            return func(conn, *args, **kwargs) # Execute without caching if no query string

        # Create a cache key using the query and any other relevant parameters
        # For a simple SELECT, the query string itself is often enough.
        # For parameterized queries, combine query with its parameters.
        # We need to make sure parameters are hashable, so convert lists/dicts if necessary.
        cache_key_parts = [sql_query]
        # Include relevant parameters in the cache key
        for arg in args:
            cache_key_parts.append(arg)
        # Sort kwargs to ensure consistent key generation
        for k, v in sorted(kwargs.items()):
            if k != 'query': # Don't duplicate 'query' in the key
                cache_key_parts.append((k, v))

        cache_key = tuple(cache_key_parts)


        if cache_key in query_cache:
            logging.info(f"--- Cache Hit for {func.__name__} ---")
            # Return a copy to prevent external modification of cached data
            return [sqlite3.Row(row) for row in query_cache[cache_key]] if query_cache[cache_key] else []
        else:
            logging.info(f"--- Cache Miss for {func.__name__} ---")
            # Execute the original function if not in cache
            result = func(conn, *args, **kwargs)

            # Cache the result. sqlite3.Row objects are not directly mutable/hashable
            # for long-term storage if they come from cursor.fetchall().
            # Convert them to tuples or lists of their values if they are from fetchall()
            # to be truly cacheable. Or store the raw values.
            # Assuming results are lists of sqlite3.Row objects:
            if isinstance(result, list) and all(isinstance(r, sqlite3.Row) for r in result):
                # Convert rows to plain tuples for caching
                cacheable_result = [tuple(row) for row in result]
            else:
                cacheable_result = result # Store as is if not rows

            query_cache[cache_key] = cacheable_result
            logging.info(f"--- Cached result for {func.__name__} ---")

            # Return a copy of the actual result to the caller (re-materialize sqlite3.Row if needed)
            return [sqlite3.Row(row) for row in cacheable_result] if cacheable_result else []

    return wrapper

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
        try:
            cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Blandine", "blandine@example.com"))
            cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Alice", "alice@example.com"))
            conn.commit()
            logging.info("Inserted test users (if not present).")
        except sqlite3.IntegrityError:
            logging.info("Test users already exist in users table.")
            conn.rollback()
    except sqlite3.Error as e:
        logging.error(f"Error setting up database: {e}")
    finally:
        if conn:
            conn.close()

# Call setup before operations
setup_database()

@with_db_connection
@cache_query
def fetch_users_with_cache(conn, query, user_id=None): # Added user_id for parameterized query example
    """
    Fetches users from the database, demonstrating caching.
    """
    logging.info(f"Executing actual database query: '{query}' with user_id={user_id}")
    cursor = conn.cursor()
    if user_id:
        cursor.execute(query, (user_id,))
    else:
        cursor.execute(query)
    return cursor.fetchall()

#### First call will cache the result
print("\n--- First call: SELECT * FROM users (should be a cache miss) ---")
start_time = time.time()
users = fetch_users_with_cache(query="SELECT * FROM users")
end_time = time.time()
print(f"Fetched Users (1st call, took {end_time - start_time:.4f}s):")
for user in users:
    print(user)
print(f"Cache state: {query_cache}")


#### Second call will use the cached result
print("\n--- Second call: SELECT * FROM users (should be a cache hit) ---")
start_time = time.time()
users_again = fetch_users_with_cache(query="SELECT * FROM users")
end_time = time.time()
print(f"Fetched Users (2nd call, took {end_time - start_time:.4f}s):")
for user in users_again:
    print(user)
print(f"Cache state: {query_cache}")


print("\n--- Third call: SELECT * FROM users WHERE id = ? (new query, cache miss) ---")
start_time = time.time()
specific_user = fetch_users_with_cache(query="SELECT * FROM users WHERE id = ?", user_id=1)
end_time = time.time()
print(f"Fetched Specific User (3rd call, took {end_time - start_time:.4f}s):")
if specific_user:
    for user in specific_user:
        print(user)
else:
    print("No specific user found.")
print(f"Cache state: {query_cache}")


print("\n--- Fourth call: SELECT * FROM users WHERE id = ? (same specific query, cache hit) ---")
start_time = time.time()
specific_user_again = fetch_users_with_cache(query="SELECT * FROM users WHERE id = ?", user_id=1)
end_time = time.time()
print(f"Fetched Specific User (4th call, took {end_time - start_time:.4f}s):")
if specific_user_again:
    for user in specific_user_again:
        print(user)
else:
    print("No specific user found.")
print(f"Cache state: {query_cache}")