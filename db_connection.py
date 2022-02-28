from math import prod
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
         segment    TEXT    NOT NULL,
         produced_time TEXT NOT NULL,
         consumed_time TEXT NOT NULL
         );''')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.connection.close()

    def close(self):
        """Close the connection to the db"""
        self.connection.close()

    def log_message(self, timestamp, road_name, segment, produced_time, consumed_time):
        """Log a message to the DB

        Args:
            timestamp (string): timestamp
            road_name (string): Name of the road
            segment (string): Details of the segment
            produced_time (string): timestamp when the message was produced 
            consumed_time (string): timestamp when the message was consumed
        """

        cur = self.connection.cursor()
        cur.execute("INSERT INTO log VALUES (?, ?, ?, ?, ?)", (timestamp, road_name, segment, produced_time, consumed_time))
        self.connection.commit()

    def read_time_data(self):
        cur = self.connection.cursor()
        cur.execute("SELECT produced_time, consumed_time from log;")
        data = cur.fetchall()
        return data
