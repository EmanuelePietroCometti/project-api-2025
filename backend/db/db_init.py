import sqlite3

DB_NAME = "database.db"

def initialize_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE NOT NULL,
            parent_id INTEGER,
            parent TEXT NOT NULL,
            name TEXT NOT NULL,
            is_dir BOOLEAN NOT NULL,
            size INTEGER,
            mtime INTEGER,
            permissions TEXT,
            version INTEGER DEFAULT 1,
            FOREIGN KEY (parent_id) REFERENCES files(id) ON DELETE CASCADE
        );"""
    )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_parent ON files(parent);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_path ON files(path);")

    conn.commit()
    conn.close()
    print(f"Database '{DB_NAME}' inizializzato correttamente con tabella 'files' e indici.")

if __name__ == "__main__":
    initialize_database()
