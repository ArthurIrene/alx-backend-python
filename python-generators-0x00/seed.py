import mysql.connector
from mysql.connector import Error
import uuid
import csv

# Connect to MySQL server (no specific DB yet)
def connect_db():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="alx_user",
            password="alx_password"
        )
        if connection.is_connected():
            print("Connected to MySQL server")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL server: {e}")
        return None

# Create database ALX_prodev if it doesn't exist
def create_database(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
        print("Database ALX_prodev created or already exists")
        cursor.close()
    except Error as e:
        print(f"Error creating database: {e}")

# Connect to the ALX_prodev database
def connect_to_prodev():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="alx_user",
            password="alx_password",
            database="ALX_prodev"
        )
        if connection.is_connected():
            print("Connected to ALX_prodev database")
        return connection
    except Error as e:
        print(f"Error connecting to ALX_prodev database: {e}")
        return None

# Create the user_data table with fields as specified
def create_table(connection):
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_data (
            user_id CHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            age DECIMAL(3,0) NOT NULL,
            INDEX (user_id)
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'user_data' created or already exists.")
        cursor.close()
    except Error as e:
        print(f"Error creating table: {e}")

# Insert data from CSV into the database, generate user_id (UUID)
def insert_data(connection, data):
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO user_data (user_id, name, email, age)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            email = VALUES(email),
            age = VALUES(age)
        """
        for row in data:
            user_id = str(uuid.uuid4())  # Generate UUID for user_id
            name = row.get('name')
            email = row.get('email')
            age = row.get('age')

            if not (name and email and age):
                print(f"Skipping incomplete row: {row}")
                continue

            cursor.execute(insert_query, (
                user_id,
                name,
                email,
                age
            ))
        connection.commit()
        print("Data inserted successfully.")
        cursor.close()
    except Error as e:
        print(f"Error inserting data: {e}")

# Read CSV file and return list of dictionaries
def read_csv(filename):
    try:
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            data = [row for row in reader]
        return data
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

if __name__ == "__main__":
    # Step 1: Connect to MySQL server
    conn = connect_db()
    if conn:
        # Step 2: Create database
        create_database(conn)
        conn.close()

    # Step 3: Connect to ALX_prodev database
    conn_prodev = connect_to_prodev()
    if conn_prodev:
        # Step 4: Create table
        create_table(conn_prodev)

        # Step 5: Read CSV data
        csv_data = read_csv("user_data.csv")

        # Step 6: Insert data into the table
        insert_data(conn_prodev, csv_data)

        # Close connection
        conn_prodev.close()
