import sqlite3
import json


def update_ship(id, ship_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            UPDATE Ship
                SET
                    name = ?,
                    hauler_id = ?
            WHERE id = ?
            """,
            (ship_data["name"], ship_data["hauler_id"], id),
        )

        rows_affected = db_cursor.rowcount

    return True if rows_affected > 0 else False


def delete_ship(pk):
    with sqlite3.connect("./shipping.db") as conn:  # creating connection to database
        conn.row_factory = sqlite3.Row
        db_cursor = (
            conn.cursor()
        )  # curser takes in string of SQL, changes it database format and gives it to us

        # Write the SQL query to get the information you want
        db_cursor.execute(
            """
        DELETE FROM Ship WHERE id = ?
        """,
            (pk,),
        )
        number_of_rows_deleted = db_cursor.rowcount

    return True if number_of_rows_deleted > 0 else False


def list_ships(url):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()  # curspr connects to SQL and gets the rows

        if url["query_params"]:
            if url["query_params"]["_expand"] == ["hauler"]:

                db_cursor.execute(
                    """
                    SELECT
                        s.id,
                        s.name,
                        s.hauler_id,
                        h.id AS haulerId,
                        h.name AS haulerName,
                        h.dock_id
                    FROM Ship s
                    JOIN Hauler h ON h.id = s.hauler_id
                """
                )

        else:
            db_cursor.execute(
                """
                SELECT
                    s.id,
                    s.name,
                    s.hauler_id
                FROM Ship s
            """
            )

        query_results = (
            db_cursor.fetchall()
        )  # fetching all the results- list of dictionaries

        # Convert rows into a list of dictionaries with additional logic to append hauler info
        ships = []

        # Check if hauler expansion is requested
        if url["query_params"].get("_expand") == ["hauler"]:
            # Loop through the query results and build ship with hauler information
            for row in query_results:
                hauler = {
                    "id": row["haulerId"],
                    "name": row["haulerName"],
                    "dock_id": row["dock_id"],
                }
                ship = {
                    "id": row["id"],
                    "name": row["name"],
                    "hauler_id": row["hauler_id"],
                    "hauler": hauler,  # Add hauler information to the ship
                }
                ships.append(ship)
        else:
            # If no hauler information is requested, add ships without hauler info
            for row in query_results:
                ship = {
                    "id": row["id"],
                    "name": row["name"],
                    "hauler_id": row["hauler_id"],
                }
                ships.append(ship)

        # Serialize Python list to JSON encoded string
        serialized_ships = json.dumps(ships)

    return serialized_ships


def retrieve_ship(pk, url):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Check if _expand is in the query params
        if "_expand" in url["query_params"]:
            if "hauler" in url["query_params"]["_expand"]:
                # Write the SQL query to get the information you want (with Hauler data)
                db_cursor.execute(
                    """
                    SELECT
                        s.id,
                        s.name,
                        s.hauler_id,
                        h.id AS haulerId,
                        h.name AS haulerName,
                        h.dock_id
                    FROM Ship s
                    JOIN Hauler h ON h.id = s.hauler_id
                    WHERE s.id = ?
                    """,
                    (pk,)
                )
            else:
                # Write the SQL query to get the ship information (without Hauler data)
                db_cursor.execute(
                    """
                    SELECT
                        s.id,
                        s.name,
                        s.hauler_id
                    FROM Ship s
                    WHERE s.id = ?
                    """,
                    (pk,)
                )
        else:
            # Write the SQL query to get the ship information (without Hauler data)
            db_cursor.execute(
                """
                SELECT
                    s.id,
                    s.name,
                    s.hauler_id
                FROM Ship s
                WHERE s.id = ?
                """,
                (pk,)
            )

        # Fetch a single row from the query results
        query_results = db_cursor.fetchone()

        if query_results:
            # If hauler data is requested, include it
            if "_expand" in url["query_params"] and "hauler" in url["query_params"]["_expand"]:
                hauler = {
                    "id": query_results["haulerId"],
                    "name": query_results["haulerName"],
                    "dock_id": query_results["dock_id"],
                }
                ship = {
                    "id": query_results["id"],
                    "name": query_results["name"],
                    "hauler_id": query_results["hauler_id"],
                    "hauler": hauler,  # Add hauler information to the ship
                }
            else:
                # If no hauler data requested, return ship without it
                ship = {
                    "id": query_results["id"],
                    "name": query_results["name"],
                    "hauler_id": query_results["hauler_id"],
                }

            # Serialize the ship object into a JSON encoded string
            serialized_ship = json.dumps(ship)

        else:
            # If no result found, return a message
            serialized_ship = json.dumps({"error": "Ship not found"})

    return serialized_ship


def create_ship(ship_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            INSERT INTO Ship (name,hauler_id)
            VALUES(?,?)
            """,
            (ship_data["name"], ship_data["hauler_id"]),
        )

    return True if db_cursor.rowcount > 0 else False
