import sqlite3
import logging
from datetime import datetime # Retained for consistency with prior tasks

# Configure basic logging for visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseConnection:
    """
    A class-based context manager for SQLite database connections.
    It automatically handles opening and closing the database connection,
    and manages transactions by committing on success or rolling back on error.
    """
    def __init__(self, db_name='users.db'):
        """
        Initializes the context manager with the database file name.
        """
        self.db_name = db_name
        self.conn = None # Connection object will be stored here

    def __enter__(self):
        """
        This method is called when entering the 'with' statement block.
        It opens the database connection and returns it.
        """
        logging.info(f"--- Entering context: Opening database connection to {self.db_name} ---")
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.conn.row_factory = sqlite3.Row # Allows accessing columns by name (e.g., row['name'])
            return self.conn # The object returned here is assigned to the 'as' variable in the with statement
        except sqlite3.Error as e:
            logging.error(f"Error opening database connection: {e}")
            raise # Re-raise the exception to propagate the error

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        This method is called when exiting the 'with' statement block.
        It ensures the database connection is closed, and handles exceptions.

        Args:
            exc_type: The type of exception (e.g., ValueError, sqlite3.Error), or None if no exception.
            exc_val: The exception instance, or None.
            exc_tb: The traceback object, or None.
        """
        if self.conn: # Ensure a connection was actually established
            if exc_type is not None:
                # An exception occurred inside the 'with' block
                logging.error(f"--- Exiting context: Exception '{exc_type.__name__}' occurred. Rolling back... ---")
                self.conn.rollback() # Rollback changes on error
            else:
                # No exception, commit changes
                logging.info(f"--- Exiting context: Committing changes and closing connection ---")
                self.conn.commit() # Commit changes if no error

            self.conn.close()
            logging.info(f"--- Database connection to {self.db_name} closed. ---")
        return False # Returning False propagates any exception that occurred within the 'with' block.


# --- Database Setup (Ensures users.db and table exist for testing) ---
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
        # Insert test data if not already present
        try:
            cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Blandine", "blandine@example.com"))
            cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Alice", "alice@example.com"))
            conn.commit()
            logging.info("Inserted test users (if not present).")
        except sqlite3.IntegrityError:
            logging.info("Test users already exist in users table.")
            conn.rollback() # Rollback if insert failed due to unique constraint
    except sqlite3.Error as e:
        logging.error(f"Error setting up database: {e}")
    finally:
        if conn:
            conn.close()

# Ensure the database is ready before running queries
setup_database()

# --- Use the context manager with the 'with' statement to perform a query ---

print("\n--- Performing SELECT * FROM users using DatabaseConnection context manager ---")
try:
    # Use the context manager: 'conn' here is the database connection object
    # returned by the __enter__ method.
    with DatabaseConnection('users.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        results = cursor.fetchall()

        print("\nQuery Results:")
        if results:
            for row in results:
                # Access columns by name, thanks to conn.row_factory = sqlite3.Row
                print(f"ID: {row['id']}, Name: {row['name']}, Email: {row['email']}")
        else:
            print("No users found.")

except Exception as e:
    logging.error(f"An error occurred during SELECT operation: {e}")

# You can also use it for other operations, like inserts or updates,
# where the commit/rollback logic will automatically apply:
print("\n--- Demonstrating an INSERT operation with the context manager ---")
try:
    with DatabaseConnection('users.db') as conn:
        cursor = conn.cursor()
        # Check if 'New User' already exists to avoid IntegrityError on repeated runs
        cursor.execute("SELECT COUNT(*) FROM users WHERE name = ?", ("Example User",))
        if cursor.fetchone()[0] == 0:
            cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Example User", "example@example.com"))
            logging.info("Successfully inserted 'Example User'. This should commit.")
        else:
            logging.info("'Example User' already exists, skipping insert.")
except Exception as e:
    logging.error(f"Error during example insert: {e}")

print("\n--- Verifying the 'Example User' was inserted ---")
try:
    with DatabaseConnection('users.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE name = ?", ("Example User",))
        user_verified = cursor.fetchone()
        if user_verified:
            print(f"Found 'Example User': ID {user_verified['id']}, Email: {user_verified['email']}")
        else:
            print("'Example User' not found (insert might have been skipped or rolled back).")
except Exception as e:
    logging.error(f"Error verifying 'Example User': {e}")