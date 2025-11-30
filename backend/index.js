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
import { Server as SocketIOServer } from 'socket.io';
import http from 'http';
import chokidar from 'chokidar';
import FileDAO from './dao/fileDAO.js';
import { stat } from 'fs';
import path from 'path';

export const ROOT_DIR = path.join(process.cwd(), "storage");
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

async function buildMetadataPayload(absPath) {
  const stats = await fs.stat(absPath);
  const relPath = clean(absPath);
  const name = path.basename(absPath);
  const parent = path.dirname(relPath);
  const permissions = (stats.mode & 0o777).toString(8);

  return {
    relPath,
    name,
    parent,
    is_dir: stats.isDirectory(),
    size: stats.size,
    mtime: Math.floor(stats.mtimeMs / 1000),
    permissions
  };
}

export const backendChanges = new Set();

const watcher = chokidar.watch(ROOT_DIR, {
  persistent: true,
  ignoreInitial: true,
  depth: 10,
  alwaysStat: true,
  awaitWriteFinish: {
    stabilityThreshold: 200,
    pollInterval: 100
  },
});

async function handleFileUpdate(pathFile, stats) {
  try {
    console.log(`Handling update for ${path}`);
    if (!stats) {
      stats = await fs.stat(pathFile);
    }
    const relPath = clean(pathFile);
    const parentPath = path.dirname(relPath);
    const name = path.basename(pathFile);
    const permissions = (stats.mode & 0o777).toString(8);



    const fileData = {
      path: relPath,
      name,
      parent: parentPath,
      is_dir: stats.isDirectory(),
      size: stats.size,
      mtime: Math.floor(stats.mtimeMs / 1000),
      permissions
    }
    console.log('File data to update:', fileData);
    await f.updateFile(fileData);
  } catch (err) {
    console.error(`Error handling update for ${pathFile}:`, err);
  }
}
async function handleRename(oldAbs, newAbs) {
  try {
    const oldRel = clean(oldAbs);
    const newRel = clean(newAbs);
    console.log(`Handling rename: ${oldRel} → ${newRel}`);
    const parts = newRel.split("/");
    const newParent = parts.length === 1 ? "." : parts.slice(0, -1).join("/");
    const newName = parts[parts.length - 1];
    const result = await f.rename(oldRel, newRel);

    if (result?.error) {
      console.error(`Rename failed: ${result.error}`);
      return;
    }

    console.log(`Rename updated in DB: ${oldRel} → ${newRel}`);

  } catch (err) {
    console.error(`Error handling rename of ${oldAbs}:`, err);
  }
}


async function handleFileDeletion(pathFile) {
  try {
    console.log(`Handling deletion for ${pathFile}`);
    await f.deleteFile(clean(pathFile));
  } catch (err) {
    console.error(`Error handling deletion for ${pathFile}:`, err);
  }
}
let pendingUnlink = null;
let pendingUnlinkDir = null;
const f = new FileDAO();
watcher
  .on('add', async fPath => {
    console.log("File added:", fPath);

    if (backendChanges.has(fPath)) {
      backendChanges.delete(fPath); return;
    }

    if (pendingUnlink && pendingUnlink.path !== fPath) {
      clearTimeout(pendingUnlink.timer);
      const oldAbs = pendingUnlink.path;
      pendingUnlink = null;

      await handleRename(oldAbs, fPath);

      const meta = await buildMetadataPayload(fPath);

      io.emit('fs_change', {
        op: 'rename',
        oldPath: clean(oldAbs),
        newPath: meta.relPath,
        ...meta
      });
      return;
    }

    await handleFileUpdate(fPath);
    const meta = await buildMetadataPayload(fPath);

    io.emit('fs_change', {
      op: 'add',
      ...meta
    });
  })
  .on('write', async fPath => {
    if (backendChanges.has(fPath)) { backendChanges.delete(fPath); return; }

    console.log('File written:', fPath);

    await handleFileUpdate(fPath);
    const meta = await buildMetadataPayload(fPath);

    io.emit('fs_change', {
      op: 'write',
      ...meta
    });
  })

  .on('addDir', async fPath => {
    if (backendChanges.has(fPath)) { backendChanges.delete(fPath); return; }

    if (pendingUnlinkDir && pendingUnlinkDir.path !== fPath) {
      clearTimeout(pendingUnlinkDir.timer);

      const oldAbs = pendingUnlinkDir.path;
      pendingUnlinkDir = null;

      await handleRename(oldAbs, fPath);

      const meta = await buildMetadataPayload(fPath);

      io.emit("fs_change", {
        op: "renameDir",
        oldPath: clean(oldAbs),
        newPath: meta.relPath,
        ...meta
      });
      return;
    }

    await handleFileUpdate(fPath);
    const meta = await buildMetadataPayload(fPath);

    meta.is_dir = true; // sicurezza

    io.emit("fs_change", {
      op: "addDir",
      ...meta
    });
  })


  .on('unlink', async fPath => {
    if (backendChanges.has(fPath)) { backendChanges.delete(fPath); return; }

    if (pendingUnlink) clearTimeout(pendingUnlink.timer);

    pendingUnlink = {
      path: fPath,
      timer: setTimeout(async () => {
        if (!pendingUnlink || pendingUnlink.path !== fPath) return;

        pendingUnlink = null;
        const rel = clean(fPath);
        const name = path.basename(fPath);
        const parent = path.dirname(rel);

        await handleFileDeletion(fPath);

        io.emit('fs_change', {
          op: 'unlink',
          relPath: rel,
          name,
          parent,
          is_dir: false,
          size: 0,
          mtime: 0,
          permissions: "000"
        });
      }, 200)
    };
  })

  .on('unlinkDir', async fPath => {
    if (backendChanges.has(fPath)) { backendChanges.delete(fPath); return; }

    if (pendingUnlinkDir) clearTimeout(pendingUnlinkDir.timer);

    pendingUnlinkDir = {
      path: fPath,
      timer: setTimeout(async () => {
        if (!pendingUnlinkDir || pendingUnlinkDir.path !== fPath) return;

        pendingUnlinkDir = null;

        const rel = clean(fPath);
        const name = path.basename(fPath);
        const parent = path.dirname(rel);

        await handleFileDeletion(fPath);

        io.emit("fs_change", {
          op: "unlinkDir",
          relPath: rel,
          name,
          parent,
          is_dir: true,
          size: 0,
          mtime: 0,
          permissions: "000"
        });
      }, 200)
    };
  })

  .on('change', async fPath => {
    if (backendChanges.has(fPath)) { backendChanges.delete(fPath); return; }

    console.log("File changed:", fPath);
    await handleFileUpdate(fPath);

    const meta = await buildMetadataPayload(fPath);

    io.emit('fs_change', {
      op: 'change',
      ...meta
    });
  })
  .on('rename', async (oldP, newP) => {
    if (backendChanges.has(newP) || backendChanges.has(oldP)) {
      backendChanges.delete(newP);
      backendChanges.delete(oldP);
      return;
    }

    console.log(`Renamed from ${oldP} to ${newP}`);
    await handleRename(oldP, newP);

    const meta = await buildMetadataPayload(newP);

    io.emit('fs_change', {
      op: 'rename',
      oldPath: clean(oldP),
      newPath: meta.relPath,
      ...meta
    });
  });



function clean(path) {
  return path.replace(/^.*storage\//, "");
}

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

// Add the socket.io server
const httpServer = http.createServer(app);
export const io = new SocketIOServer(httpServer, {
  cors: {
    origin: originAddress,
    credentials: true
  }
});

io.on('connection', (socket) => {
  console.log('A user connected:', socket.id);

  socket.on('disconnect', () => {
    console.log('User disconnected:', socket.id);
  });
});


// activate the server
httpServer.listen(port, () => {
  console.log(`Server listening at ${originAddress}`);
});
