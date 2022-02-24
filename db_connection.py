import sqlite3


class DBConnection:
    """Represents a connection to a sqlite3 database."""

    def __init__(self):
        """Init a connection to the database"""
        self.connection = sqlite3.connect("db.db")
        """Create the table to log traffic info"""
        self.connection.execute('''CREATE TABLE IF NOT EXISTS log
         (timestamp TEXT    NOT NULL,
         road_name  TEXT    NOT NULL,
         segment    TEXT    NOT NULL
         );''')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.connection.close()

    def close(self):
        """Close the connection to the db"""
        self.connection.close()

    def log_message(self, timestamp, road_name, segment):
        """Log a message to the DB

        Args:
            timestamp (string): timestamp
            road_name (string): Name of the road
            segment (string): Details of the segment
        """

        cur = self.connection.cursor()
        cur.execute("INSERT INTO log VALUES (?, ?, ?)", (timestamp, road_name, segment))
        self.connection.commit()
