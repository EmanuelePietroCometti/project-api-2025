import express from 'express';
import morgan from 'morgan';
import cors from 'cors';
import listRoutes from './routes/listRoutes.js';
import filesRoutes from './routes/filesRoutes.js';
import mkdirRoutes from './routes/mkdirRoutes.js';
import statsRoutes from './routes/statsRoutes.js';
import os from 'os';
import fs from 'fs/promises';
import db from './db/fileDB.js';

const ROOT_DIR = './storage';
async function bootstrap(rootDir, dbConnection) {
  console.log('Running startup routine...');
  try {
    await fs.mkdir(rootDir, { recursive: true });
    console.log(`Root directory created or already exists: ${rootDir}`);
  } catch (err) {
    console.error('Error creating directories:', err);
    throw err;
  }
  const createTableQuery = `
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL UNIQUE,
            parent_id INTEGER,
            parent TEXT,
            name TEXT,
            is_dir BOOLEAN,
            size INTEGER,
            mtime INTEGER,
            permissions TEXT,
            version INTEGER DEFAULT 1,
            FOREIGN KEY(parent_id) REFERENCES files(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent);
    `;
    return new Promise((resolve, reject) => {
        // dbConnection è il tuo oggetto db (es. sqlite3.Database)
        dbConnection.exec(createTableQuery, (err) => { 
            if (err) {
                console.error("Error creating DB table:", err);
                return reject(err);
            }
            console.log("Table 'files' created in DB.");
            resolve();
        });
    });
}

await bootstrap(ROOT_DIR, db);

// init express
const app = new express();
app.use(morgan('dev'));
app.use(express.json());

const port = 3001;
const interfaces = os.networkInterfaces(); 
const addresses = [];

for (let iface in interfaces) {
  for (let addr of interfaces[iface]) {
    if (addr.family === "IPv4" && !addr.internal) {
      addresses.push(addr.address);
    }
  }
}
const originAddress = `http://${addresses[0]}:5173/`;

const corsOptions = {
  origin: originAddress,
  credentials: true
};
app.use(cors(corsOptions));

// API
app.use('/list', listRoutes);
app.use('/files', filesRoutes);
app.use('/mkdir', mkdirRoutes);
app.use('/stats', statsRoutes);


// activate the server
app.listen(port, () => {
  console.log(`Server listening at ${originAddress}`);
});
