import sqlite3

# Nome del database SQLite
DB_NAME = "database.db"


def initialize_database():
    # Connessione al database (verrà creato se non esiste)
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Creazione della tabella "files"
    cursor.execute(
        """CREATE TABLE files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE NOT NULL,
            parent INTEGER,
            name TEXT NOT NULL,
            is_dir BOOLEAN NOT NULL,
            size INTEGER,
            mtime INTEGER,
            permissions TEXT,
            version INTEGER DEFAULT 1,
            FOREIGN KEY (parent) REFERENCES files(id) ON DELETE CASCADE
        );"""
    )

    # Creazione degli indici
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_parent ON files(parent);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_path ON files(path);")

    # Commit e chiusura connessione
    conn.commit()
    conn.close()
    print(
        f"Database '{DB_NAME}' inizializzato correttamente con tabella 'files' e indici."
    )


if __name__ == "__main__":
    initialize_database()
