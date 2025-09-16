import sqlite3 from "sqlite3";


const database_path = './db'
const db = new sqlite3.Database(database_path+'/database.db', (err) => {
    if (err){throw err} ;
});

export default db;