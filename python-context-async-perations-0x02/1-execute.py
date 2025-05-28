import sqlite3
import logging
from datetime import datetime # Included for completeness and consistency

# Configure basic logging for visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ExecuteQuery:
    """
    A class-based context manager that takes a SQL query and its parameters,
    executes it within a database connection, and returns the results.
    It manages connection opening/closing and transaction handling (commit/rollback).
    """
    def __init__(self, db_name, query, params=()):
        """
        Initializes the context manager with the database name, SQL query,
        and optional parameters for the query.
        """
        self.db_name = db_name
        self.query = query
        self.params = params
        self.conn = None      # To hold the database connection
        self.results = None   # To hold the results of the query

    def __enter__(self):
        """
        Establishes the database connection, executes the query, and stores/returns results.
        """
        logging.info(f"--- Entering context: Opening DB for query: '{self.query}' with params: {self.params} ---")
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.conn.row_factory = sqlite3.Row # Allows accessing columns by name
            cursor = self.conn.cursor()

            # Execute the parameterized query
            cursor.execute(self.query, self.params)

            # Fetch and store results
            self.results = cursor.fetchall()
            return self.results # Return results directly for use in the 'with' statement
        except sqlite3.Error as e:
            logging.error(f"Error executing query in __enter__: {e}")
            self.results = [] # Ensure results is an empty list on error
            raise # Re-raise the exception to propagate it

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensures the database connection is closed and handles transaction commit/rollback.
        """
        if self.conn: # Check if connection was successfully established
            if exc_type is not None:
                # An exception occurred inside the 'with' block
                logging.error(f"--- Exiting context: Exception '{exc_type.__name__}' occurred during query. Rolling back... ---")
                self.conn.rollback() # Rollback changes on error
            else:
                # No exception, query completed successfully
                logging.info(f"--- Exiting context: Query completed successfully. Committing changes... ---")
                self.conn.commit() # Commit changes (harmless for SELECT, crucial for INSERT/UPDATE)

            self.conn.close() # Always close the connection
            logging.info(f"--- Database connection to {self.db_name} closed. ---")

        return False # Propagate any exception that occurred within the 'with' block


# --- Database Setup (Modified to include 'age' column) ---
def setup_database_with_age():
    conn = None
    try:
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()

        # Create table if it doesn't exist, including 'age' column
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER
            )
        ''')
        conn.commit()

        # Add 'age' column if it doesn't exist in existing table (for seamless updates)
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN age INTEGER")
            conn.commit()
            logging.info("Added 'age' column to users table.")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                logging.info("'age' column already exists in users table.")
            else:
                raise # Re-raise other operational errors

        # Insert test data if not already present, ensuring age values
        users_to_insert = [
            ("Blandine", "blandine@example.com", 30),
            ("Alice", "alice@example.com", 22),
            ("Charles", "charles@example.com", 45),
            ("Diana", "diana@example.com", 25)
        ]
        for name, email, age in users_to_insert:
            try:
                cursor.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", (name, email, age))
                conn.commit()
                logging.info(f"Inserted user: {name} (age: {age}).")
            except sqlite3.IntegrityError:
                logging.info(f"User '{name}' already exists.")
                conn.rollback() # Rollback if insert failed due to unique constraint
    except sqlite3.Error as e:
        logging.error(f"Error setting up database: {e}")
    finally:
        if conn:
            conn.close()

# Ensure the database is ready with an 'age' column and data
setup_database_with_age()

# --- Use the ExecuteQuery context manager ---

print("\n--- Using ExecuteQuery for 'SELECT * FROM users WHERE age > ?' ---")
# Define the query and parameter
query_to_execute = "SELECT * FROM users WHERE age > ?"
query_parameter = (25,) # Parameter must be an iterable (tuple or list)

try:
    # 'users_over_25' will receive the results returned by __enter__()
    with ExecuteQuery('users.db', query_to_execute, query_parameter) as users_over_25:
        print(f"\nUsers older than {query_parameter[0]}:")
        if users_over_25:
            for user in users_over_25:
                print(f"ID: {user['id']}, Name: {user['name']}, Email: {user['email']}, Age: {user['age']}")
        else:
            print("No users found matching the criteria.")

except Exception as e:
    logging.error(f"An error occurred when executing the query: {e}")

print("\n--- Demonstrating an ExecuteQuery with no matching results ---")
try:
    with ExecuteQuery('users.db', "SELECT * FROM users WHERE age > ?", (100,)) as very_old_users:
        print("Users older than 100:")
        if very_old_users:
            for user in very_old_users:
                print(f"ID: {user['id']}, Name: {user['name']}, Email: {user['email']}, Age: {user['age']}")
        else:
            print("No users found older than 100.")
except Exception as e:
    logging.error(f"An error occurred when executing the query: {e}")


print("\n--- Demonstrating error handling with ExecuteQuery (e.g., malformed query) ---")
try:
    # This query has a syntax error
    with ExecuteQuery('users.db', "SELECT * FROM users WHERE ag_e > ?", (20,)) as invalid_query_results:
        print("This line should not be reached if an error occurs.")
except sqlite3.OperationalError as e:
    print(f"\nCaught expected error outside context manager: {e}")
    logging.info("As expected, the sqlite3.OperationalError propagated.")
except Exception as e:
    print(f"\nCaught an unexpected error: {e}")