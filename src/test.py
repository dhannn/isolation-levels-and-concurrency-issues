from transactions import initialize_connection, transfer_funds_to_multiple
from dotenv import load_dotenv

import os

load_dotenv()

conn = {
    'host': os.environ['DB_HOST'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'database': 'isolation_levels_exercise'
}

connection = initialize_connection(conn)

transfer_funds_to_multiple(connection, 1, {2: 10, 3: 50})
