import sqlite3
import json

def update_hauler(id, hauler_data): #hauler_data: A dictionary containing the new data for the hauler. The dictionary should have keys name and dock_id (which are columns in the Hauler table).
    with sqlite3.connect("./shipping.db") as conn: #A connection to the shipping.db SQLite database is established using sqlite3.connect("./shipping.db").
        db_cursor = conn.cursor() #conn is the SQLite connection object (which is established by sqlite3.connect()).
                                    #cursor() is a method on the connection object that returns a cursor object, which is used to execute SQL queries against the database
        db_cursor.execute(
            """
            UPDATE Hauler              
                SET
                    name = ?,
                    dock_id = ?
            WHERE id = ?
            """,
            (hauler_data['name'], hauler_data['dock_id'], id) #tuple of Values/new name/dock ID for the hauler, extracted from the hauler_data dictionary.
        )

        rows_affected = db_cursor.rowcount #db_cursor.rowcount is used to check how many rows were affected by the query. If a row was updated, rows_affected will be greater than 0.

    return True if rows_affected > 0 else False


def delete_hauler(pk):

    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute("""
        DELETE FROM Hauler WHERE id = ?
        """, (pk,)
        )
        number_of_rows_deleted = db_cursor.rowcount

    return True if number_of_rows_deleted > 0 else False


def list_haulers():
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute("""
        SELECT
            h.id,
            h.name,
            h.dock_id
        FROM Hauler h
        """)
        query_results = db_cursor.fetchall()

        # Initialize an empty list and then add each dictionary to it
        haulers=[]
        for row in query_results:
            haulers.append(dict(row))

        # Serialize Python list to JSON encoded string
        serialized_haulers = json.dumps(haulers)

    return serialized_haulers

def retrieve_hauler(pk):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute("""
        SELECT
            h.id,
            h.name,
            h.dock_id
        FROM Hauler h
        WHERE h.id = ?
        """, (pk,))
        query_results = db_cursor.fetchone()

        # Serialize Python list to JSON encoded string
        serialized_hauler = json.dumps(dict(query_results))

    return serialized_hauler

def create_hauler(hauler_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            INSERT INTO Hauler (name,dock_id)
            VALUES(?,?)
            """,
            (hauler_data['name'], hauler_data['dock_id'])
        )
    
    return True if db_cursor.rowcount > 0 else False
