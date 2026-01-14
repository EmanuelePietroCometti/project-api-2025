import express from "express";
import fs from "fs";
import path from "path";
import FileDAO from "../dao/fileDAO.js";
import { ROOT_DIR, backendChanges } from '../index.js';

const router = express.Router();
const f = new FileDAO();

function parseRange(rangeHeader, fileSize) {
  const match = rangeHeader.match(/bytes=(\d*)-(\d*)/);

  let start = match[1] ? parseInt(match[1], 10) : 0;
  let end = match[2] ? parseInt(match[2], 10) : fileSize - 1;

  if (isNaN(start)) start = 0;
  if (isNaN(end)) end = fileSize - 1;

  end = Math.min(end, fileSize - 1);

  return [start, end];
}

router.get("/", async (req, res) => {
  try {
    let relPath = req.query.relPath;
    if (relPath.startsWith('././')) {
      relPath = relPath.slice(2);
    }
    const filePath = path.join(ROOT_DIR, relPath);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: "File not found" });
    }

    backendChanges.add(filePath);

    const stats = await fs.promises.stat(filePath);
    const fileSize = stats.size;

    res.setHeader("Accept-Ranges", "bytes");

    const range = req.headers.range;

    if (range) {
      const [start, end] = parseRange(range, fileSize);
      const chunkSize = end - start + 1;

      res.writeHead(206, {
        "Content-Range": `bytes ${start}-${end}/${fileSize}`,
        "Content-Length": chunkSize,
        "Content-Type": "application/octet-stream"
      });

      fs.createReadStream(filePath, { start, end }).pipe(res);
      return;
    }

    res.writeHead(200, {
      "Content-Length": fileSize,
      "Content-Type": "application/octet-stream"
    });

    fs.createReadStream(filePath).pipe(res);

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// PUT /files/path (write file)
router.put("/", async (req, res) => {
  let fd;

  try {
    let relPath = req.query.relPath;
    if (relPath.startsWith('././')) {
      relPath = relPath.slice(2);
    }
    const offset = parseInt(req.query.offset ?? "0", 10);

    const filePathAbs = path.join(ROOT_DIR, relPath);
    const parentPathAbs = path.dirname(filePathAbs);
    const parentPath = path.dirname(relPath);
    const name = path.basename(filePathAbs);

    backendChanges.add(filePathAbs);
    if (!fs.existsSync(parentPathAbs)) {
      return res.status(400).json({
        error: "Parent directory not found. Create the directory first."
      });
    }
    const flag = (offset === 0) ? "w+" : "r+";
    
    try {
        fd = await fs.promises.open(filePathAbs, flag);
    } catch (err) {
        if (err.code === 'ENOENT') {
            fd = await fs.promises.open(filePathAbs, "w+");
        } else {
            throw err;
        }
    }

    let writtenTotal = 0;
    let currentOffset = offset;

    for await (const chunk of req) {
      await fd.write(chunk, 0, chunk.length, currentOffset);
      currentOffset += chunk.length;
      writtenTotal += chunk.length;
    }

    await fd.close();
    fd = null;

    const stats = await fs.promises.stat(filePathAbs);

    await f.updateFile({
      path: relPath,
      name,
      parent: parentPath,
      is_dir: false,
      size: stats.size,
      mtime: Math.floor(stats.mtimeMs / 1000),
      permissions: (stats.mode & 0o777).toString(8),
      nlink: stats.nlink,
    });
    await f.syncMetadataFromDisk(parentPath);
    res.status(200).json({
      message: "File correctly saved.",
      written: writtenTotal
    });

  } catch (err) {
    if (fd) {
      try { await fd.close(); } catch { }
    }
    console.error(err);
    res.status(500).json({ error: "Error writing file" });
  }
});

// DELETE /files/path
router.delete("/", async (req, res) => {
  try {
    let relPath = req.query.relPath;
    if (relPath.startsWith('././')) {
      relPath = relPath.slice(2);
    }
    const filePathAbs = path.join(ROOT_DIR, relPath);
    backendChanges.add(filePathAbs);

    const stats = await fs.promises.stat(filePathAbs).catch(() => null);
    if (!stats) {
      return res.status(404).json({ error: "File or directory not found" });
    }
    const parentPath=path.dirname(relPath);

    if (stats.isDirectory()) {
      await fs.promises.rm(filePathAbs, { recursive: true, force: true });
    } else {
      await fs.promises.unlink(filePathAbs);
    }

    await f.deleteFile(relPath);
    await f.syncMetadataFromDisk(parentPath);
    res.status(200).json({ message: "Deletion completed" });
  } catch (err) {
    res.status(500).json({ error: "Internal server error" });
  }
});

// PATCH /files/chmod?relPath=...&perm=755
router.patch("/chmod", async (req, res) => {
  try {
    let relPath = req.query.relPath;
    if (relPath.startsWith('././')) {
      relPath = relPath.slice(2);
    }
    const perm = req.query.perm;
    const filePathAbs = path.join(ROOT_DIR, relPath);
    backendChanges.add(filePathAbs);
    await fs.promises.chmod(filePathAbs, parseInt(perm, 8));
    await f.updatePermissions(relPath, perm);
    res.status(200).json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: "chmod failed" });
  }
});

// PATCH /files/truncate?relPath=...&size=123
router.patch("/truncate", async (req, res) => {
  try {
    let relPath = req.query.relPath;
    if (relPath.startsWith('././')) {
      relPath = relPath.slice(2);
    }
    const size = parseInt(req.query.size, 10);
    const filePathAbs = path.join(ROOT_DIR, relPath);
    backendChanges.add(filePathAbs);
    await fs.promises.truncate(filePathAbs, size);
    const stats = await fs.promises.stat(filePathAbs);
    await f.updateFile({
      path: relPath,
      name: path.basename(filePathAbs),
      parent: path.dirname(relPath),
      is_dir: false,
      size: stats.size,
      mtime: Math.floor(stats.mtimeMs / 1000),
      permissions: (stats.mode & 0o777).toString(8),
      nlink: stats.nlink,
    });
    res.status(200).json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: "truncate failed" });
  }
});

// PATCH /files/utimes?relPath=...&atime=...&mtime=...
router.patch("/utimes", async (req, res) => {
  try {
    let relPath = req.query.relPath;
    if (relPath.startsWith('././')) {
      relPath = relPath.slice(2);
    }
    const at = req.query.atime ? parseInt(req.query.atime, 10) : null;
    const mt = req.query.mtime ? parseInt(req.query.mtime, 10) : null;
    const filePathAbs = path.join(ROOT_DIR, relPath);
    backendChanges.add(filePathAbs);

    const stats = await fs.promises.stat(filePathAbs);
    const atime = at ? new Date(at * 1000) : stats.atime;
    const mtime = mt ? new Date(mt * 1000) : stats.mtime;
    await fs.promises.utimes(filePathAbs, atime, mtime);
    const stats2 = await fs.promises.stat(filePathAbs);
    await f.updateMtime(relPath, Math.floor(stats2.mtimeMs / 1000));
    res.status(200).json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: "utimes failed" });
  }
});

// PATCH /files/rename?oldRelPath=...&newRelPath=... 
router.patch("/rename", async (req, res) => {
  try {
    let oldRelPath = req.query.oldRelPath;
    if (oldRelPath.startsWith('././')) {
      oldRelPath = oldRelPath.slice(2);
    }
    let newRelPath = req.query.newRelPath;
     if (newRelPath.startsWith('././')) {
      newRelPath = newRelPath.slice(2);
    }
    if (!oldRelPath || !newRelPath) {
      return res.status(400).json({ error: "Missing oldRelPath or newRelPath" });
    }
    const oldAbsPath = path.join(ROOT_DIR, oldRelPath);
    const newAbsPath = path.join(ROOT_DIR, newRelPath);
    const newParentDir = path.dirname(newAbsPath);
    backendChanges.add(oldAbsPath);
    backendChanges.add(newAbsPath);

    const isRestore = oldRelPath.startsWith("/.Trash-");
    try {
      await fs.promises.stat(newAbsPath);
      await fs.promises.unlink(newAbsPath);
      await f.deleteFile(newRelPath);

    } catch (err) {
      if (err.code !== 'ENOENT') {
        throw err;
      }
    }
    if (!fs.existsSync(newParentDir)) {
      if (isRestore) {
        try {
          await fs.promises.mkdir(newParentDir, { recursive: true });
        } catch (err) {
          return res.status(500).json({ error: "Failed to create destination parent directory for restore" });
        }
      } else {
        return res.status(404).json({ error: "New parent directory does not exist" });
      }
    }
    await fs.promises.rename(oldAbsPath, newAbsPath);
    await f.rename(oldRelPath, newRelPath);

    res.status(200).json({ ok: true });

  } catch (err) {
    if (err.code === 'ENOENT') {
      return res.status(404).json({ error: "File not found for rename" });
    }
    res.status(500).json({ error: "rename failed" });
  }
});

export default router;