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
import path from 'path';

export const ROOT_DIR = path.join(process.cwd(), "storage");
async function bootstrap(rootDir, dbConnection) {
  try {
    await fs.mkdir(rootDir, { recursive: true });
  } catch (err) {
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
            nlink INTEGER,
            version INTEGER DEFAULT 1,
            FOREIGN KEY(parent_id) REFERENCES files(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent);
    `;
  return new Promise((resolve, reject) => {
    dbConnection.exec(createTableQuery, (err) => {
      if (err) {
        return reject(err);
      }
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
    permissions,
    nlink: stats.nlink,
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
      permissions,
      nlink: stats.nlink
    }
    await f.updateFile(fileData);
  } catch (err) {
    console.error(`Error handling update for ${pathFile}:`, err);
  }
}
async function handleRename(oldAbs, newAbs) {
  try {
    const oldRel = clean(oldAbs);
    const newRel = clean(newAbs);
    const parts = newRel.split("/");
    const newParent = parts.length === 1 ? "." : parts.slice(0, -1).join("/");
    const newName = parts[parts.length - 1];
    const result = await f.rename(oldRel, newRel);

    if (result?.error) {
      return;
    }
  } catch (err) {
    console.error(`Error handling rename of ${oldAbs}:`, err);
  }
}


async function handleFileDeletion(pathFile) {
  try {
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

    meta.is_dir = true;

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
          permissions: "000",
          nlink: 0,
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
          permissions: "000",
          nlink: 0,
        });
      }, 200)
    };
  })

  .on('change', async fPath => {
    if (backendChanges.has(fPath)) { backendChanges.delete(fPath); return; }

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

    await handleRename(oldP, newP);

    const meta = await buildMetadataPayload(newP);

    io.emit('fs_change', {
      op: 'rename',
      oldPath: clean(oldP),
      newPath: meta.relPath,
      ...meta
    });
  });



function clean(absPath) {
  let relative = path.relative(ROOT_DIR, absPath);
  relative = relative.replace('\/g', '/');
  if (relative === '' || relative === '.') {
    return absPath;
  }
  if (!relative.startsWith('./')) {
    relative = './' + relative;
  }
  if (relative.includes('..')) {
    return '.';
  }

  return relative;
}

function getPrimaryIP() {
  const interfaces = os.networkInterfaces();

  for (const name in interfaces) {
    for (const net of interfaces[name]) {
      if (net.family === "IPv4" && !net.internal) {
        return net.address; 
      }
    }
  }

  return null;
}

// init express
const app = new express();
app.use(morgan('dev'));
app.use(express.json());

const port = 3001;
let IPAddress = process.argv[2];

if (!IPAddress) {
  IPAddress=getPrimaryIP();
  console.log("Server available at IP address: ", IPAddress);
}

const originAddress = `http://${IPAddress}:5173/`;

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
  socket.on('disconnect', () => {});
});


// activate the server
httpServer.listen(port);
