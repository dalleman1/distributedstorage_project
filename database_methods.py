import sqlite3


def create_database(cur: sqlite3.Cursor):
    cur.execute(
        """ CREATE TABLE file (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT,
        size INTEGER,
        content_type TEXT,
        storage_mode TEXT,
        storage_details TEXT,
        created DATETIME DEFAULT CURRENT_TIMESTAMP
    );"""
    )


if __name__ == "__main__":
    con = sqlite3.connect("files.db")
    cur = con.cursor()

    create_database(cur)
