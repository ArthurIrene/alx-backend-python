import mysql.connector
from mysql.connector import Error
from seed import connect_to_prodev


def stream_users_in_batches(batch_size):
    """
    Generator that yields user rows in batches of batch_size.
    """
    connection = connect_to_prodev()
    cursor = connection.cursor(dictionary=True)

    cursor.execute("SELECT * FROM user_data")

    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        yield batch

    cursor.close()
    connection.close()


def batch_processing(batch_size):
    return (
        user
        for batch in stream_users_in_batches(batch_size)
        for user in batch
        if user['age'] > 25
    )

