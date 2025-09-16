import sqlite3

# Nome del database SQLite
DB_NAME = "database.db"

def initialize_database():
    # Connessione al database (verrà creato se non esiste)
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Creazione della tabella "files"
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT UNIQUE NOT NULL,
        parent TEXT NOT NULL,
        name TEXT NOT NULL,
        is_dir BOOLEAN NOT NULL,
        size INTEGER DEFAULT 0,
        mtime INTEGER NOT NULL,
        permissions TEXT DEFAULT '0644',
        version INTEGER DEFAULT 1
    );
    """)

    # Creazione degli indici
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_parent ON files(parent);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_path ON files(path);")

    # Commit e chiusura connessione
    conn.commit()
    conn.close()
    print(f"Database '{DB_NAME}' inizializzato correttamente con tabella 'files' e indici.")

if __name__ == "__main__":
    initialize_database()
