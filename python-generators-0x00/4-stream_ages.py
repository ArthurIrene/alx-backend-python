import mysql.connector
from seed import connect_to_prodev

def stream_user_ages():
    """
    Generator to yield user ages one by one from the database.
    """
    connection = connect_to_prodev()
    cursor = connection.cursor()

    cursor.execute("SELECT age FROM user_data")

    for row in cursor:
        yield row[0]  # row is a tuple like (age,)

    cursor.close()
    connection.close()


def calculate_average_age():
    """
    Calculates and prints the average age using a generator.
    """
    total = 0
    count = 0

    for age in stream_user_ages():
        total += age
        count += 1

    if count > 0:
        average = total / count
        print(f"Average age of users: {average:.2f}")
    else:
        print("No users found.")



calculate_average_age()