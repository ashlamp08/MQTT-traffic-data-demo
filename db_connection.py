import sqlite3


class DBConnection:
    """Represents a connection to a sqlite3 database."""

    def __init__(self):
        """Init a connection to the database"""
        self.connection = sqlite3.connect("db.db")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.connection.close()

    def close(self):
        """Close the connection to the db"""
        self.connection.close()

    def log_message(self, message, sent: bool, recieved: bool):
        """Log a message to the DB

        Args:
            message (any): The message
            sent (bool): If message was sent
            recieved (bool): If message was recieved
        """

        cur = self.connection.cursor()
        cur.execute("INSERT INTO log VALUES (?, ?, ?)", (message, sent, recieved))
        self.connection.commit()
