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
        conn = None # Initialize connection to None
        try:
            logging.info(f"--- Establishing DB Connection for {func.__name__} ---")
            conn = sqlite3.connect('users.db')
            conn.row_factory = sqlite3.Row # Optional: access columns by name

            # Pass the connection object as the first argument to the original function
            # The original function expects 'conn' as its first parameter.
            result = func(conn, *args, **kwargs)

            # Note: conn.commit() is NOT here anymore.
            # It will be handled by the @transactional decorator (if present).
            return result
        except sqlite3.Error as e:
            logging.error(f"Database error in {func.__name__}: {e}")
            # Do NOT rollback here either, it's transactional's job
            raise # Re-raise the exception to propagate it
        finally:
            if conn:
                conn.close()
                logging.info(f"--- Closed DB Connection for {func.__name__} ---")

    return wrapper

#### transactional decorator (NEW for Task 2)
def transactional(func):
    """
    Decorator that ensures a function running a database operation is wrapped inside a transaction.
    If the function raises an error, it rolls back the transaction; otherwise, it commits.
    This decorator assumes the decorated function receives a 'conn' object as its first argument.
    """
    @functools.wraps(func)
    def wrapper(conn, *args, **kwargs): # 'conn' is now expected as the first arg
        if not isinstance(conn, sqlite3.Connection):
            logging.error(f"Function {func.__name__} was decorated with @transactional but did not receive a sqlite3.Connection object as its first argument.")
            raise TypeError("Expected a sqlite3.Connection object as the first argument for transactional decorator.")

        try:
            logging.info(f"--- Starting Transaction for {func.__name__} ---")
            result = func(conn, *args, **kwargs) # Execute the original function
            conn.commit() # Commit changes if no error occurred
            logging.info(f"--- Transaction Committed for {func.__name__} ---")
            return result
        except Exception as e:
            conn.rollback() # Rollback if any exception occurs
            logging.error(f"Transaction rolled back for {func.__name__} due to error: {e}")
            raise # Re-raise the exception to propagate it
    return wrapper


# --- Setup for testing (re-used from previous task) ---
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

# --- Functions to be decorated ---

# Notice the order: @with_db_connection is applied first, then @transactional
# This means @with_db_connection provides the 'conn', then @transactional uses it.
@with_db_connection
@transactional
def update_user_email(conn, user_id, new_email):
    """
    Updates a user's email in the 'users' table.
    This function expects an open database connection (conn) as its first argument.
    """
    logging.info(f"Attempting to update email for user_id={user_id} to {new_email}")
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET email = ? WHERE id = ?", (new_email, user_id))
    # Check if any rows were affected
    if cursor.rowcount == 0:
        logging.warning(f"No user found with ID {user_id} to update.")
        return False # Indicate no update happened
    logging.info(f"User ID {user_id} email updated successfully to {new_email}.")
    return True

@with_db_connection
@transactional
def delete_user(conn, user_id):
    """
    Deletes a user from the 'users' table.
    """
    logging.info(f"Attempting to delete user with ID: {user_id}")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
    if cursor.rowcount == 0:
        logging.warning(f"No user found with ID {user_id} to delete.")
        return False
    logging.info(f"User ID {user_id} deleted successfully.")
    return True

@with_db_connection
def get_user_by_id(conn, user_id):
    """
    Fetches a user by ID. (Not transactional as it's a read operation)
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    return cursor.fetchone()

# --- Test Cases ---

print("\n--- Test Case 1: Successful Email Update ---")
# Initial check
user_before = get_user_by_id(user_id=1)
if user_before:
    print(f"Before update: User ID 1 email is {user_before['email']}")
else:
    print("User ID 1 not found for pre-check.")

update_status = update_user_email(user_id=1, new_email='blandine.new@example.com')
print(f"Update Status for ID 1: {update_status}")

# Verify update
user_after = get_user_by_id(user_id=1)
if user_after:
    print(f"After successful update: User ID 1 email is {user_after['email']}")
else:
    print("User ID 1 not found for post-check.")


print("\n--- Test Case 2: Attempting to Update Non-Existent User (No Rollback needed, no changes made) ---")
update_status_non_existent = update_user_email(user_id=999, new_email='non_existent@example.com')
print(f"Update Status for ID 999: {update_status_non_existent}")


print("\n--- Test Case 3: Forced Rollback (Simulate an error during operation) ---")

@with_db_connection
@transactional
def simulate_failed_update(conn, user_id, new_email):
    cursor = conn.cursor()
    # First update (should commit if no error)
    cursor.execute("UPDATE users SET email = ? WHERE id = ?", (new_email, user_id))
    logging.info(f"Simulated first update for ID {user_id}.")

    # Simulate an error - for example, trying to divide by zero
    logging.info("Simulating an error now to trigger rollback...")
    _ = 1 / 0 # This will cause a ZeroDivisionError

    # This part would not be reached
    cursor.execute("UPDATE users SET name = ? WHERE id = ?", ("Should Not Be Saved", user_id))


# Check current state of a user (e.g., Alice)
user_alice_before = get_user_by_id(user_id=2)
if user_alice_before:
    print(f"Before simulated failure: User ID 2 email is {user_alice_before['email']}")
else:
    print("User ID 2 (Alice) not found for pre-check.")

try:
    # This call should cause a rollback
    simulate_failed_update(user_id=2, new_email='alice.failed@example.com')
except ZeroDivisionError:
    print("Caught expected ZeroDivisionError, confirming rollback should have happened.")

# Verify that the update was rolled back for Alice
user_alice_after = get_user_by_id(user_id=2)
if user_alice_after:
    print(f"After simulated failure: User ID 2 email is {user_alice_after['email']}")
else:
    print("User ID 2 (Alice) not found for post-check.")
# If rollback worked, Alice's email should still be 'alice@example.com' or original.

print("\n--- Test Case 4: Deleting a user ---")
# Add a temporary user to delete
@with_db_connection
@transactional
def add_temp_user(conn, name, email):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", (name, email))
    logging.info(f"Added temporary user: {name}")

add_temp_user(name="Temp User", email="temp@example.com")
# Find Temp User's ID
temp_user_data = get_user_by_id(query="SELECT * FROM users WHERE name = ?", name="Temp User")
temp_user_id = temp_user_data['id'] if temp_user_data else None

if temp_user_id:
    print(f"Temp User added with ID: {temp_user_id}")
    delete_status = delete_user(user_id=temp_user_id)
    print(f"Delete Status for Temp User ID {temp_user_id}: {delete_status}")

    # Verify deletion
    deleted_user = get_user_by_id(user_id=temp_user_id)
    if not deleted_user:
        print(f"Successfully verified deletion of Temp User ID {temp_user_id}.")
    else:
        print(f"Failed to delete Temp User ID {temp_user_id}.")
else:
    print("Failed to add temporary user, cannot test deletion.")