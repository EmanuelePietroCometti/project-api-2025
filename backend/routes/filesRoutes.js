import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import FileDAO from "../dao/fileDAO.js";

const router = express.Router();
const f = new FileDAO();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.join(__dirname, "..", "storage");

// GET /files/path
router.get("/", async (req, res) => {
  try {
    const relPath = req.query.relPath;
    console.log("Requested file:", relPath);
    const filePath = path.join(ROOT_DIR, relPath);
    const parentPath = path.dirname(filePath);

    // Controlla che la directory padre esista
    if (!fs.existsSync(parentPath)) {
      return res
        .status(400)
        .json({
          error: "Parent directory not found. Create the directory first.",
        });
    }
    if (fs.is_dir) {
      return res
        .status(400)
        .json({ error: "Path is referenced to a directory." });
    }

    const readStream = fs.createReadStream(filePath);
    readStream.pipe(res);

    readStream.on("error", (err) => {
      console.error(err);
      res.status(500).json({ error: "Error on file reading" });
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// PUT /files/path
router.put("/", async (req, res) => {
  try {
    const relPath = req.query.relPath;
    const filePathAbs = path.join(ROOT_DIR, relPath);
    const parentPathAbs = path.dirname(filePathAbs);
    const parentPath = path.dirname(relPath);
    const name = path.basename(filePathAbs);

    /* console.log({
      relPath,
      filePathAbs,
      parentPathAbs,
      name,
      ROOT_DIR,
    }); */

    // Controlla che la directory padre esista
    if (!fs.existsSync(parentPathAbs)) {
      return res
        .status(400)
        .json({
          error: "Parent directory not found. Create the directory first.",
        });
    }

    // Scrivi file con streaming
    const writeStream = fs.createWriteStream(filePathAbs);
    req.pipe(writeStream);

    writeStream.on("finish", async () => {
      const stats = await fs.promises.stat(filePathAbs);


      // Aggiorna o inserisci metadata nel DB
      await f.updateFile({
        path: relPath,
        name: name,
        parent: parentPath,
        is_dir: false,
        size: stats.size,
        mtime: Math.floor(stats.mtimeMs / 1000),
        permissions: "755",
      });

      res.status(200).json({ message: "File correctly saved. " });
    });

    writeStream.on("error", (err) => {
      console.error(err);
      res.status(500).json({ error: "Error on file writing" });
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// DELETE /files/path
router.delete("/", async (req, res) => {
  try {
    const relPath = req.query.relPath;
    const filePathAbs = path.join(ROOT_DIR, relPath);

    // Controlla se esiste
    const stats = await fs.promises.stat(filePathAbs).catch(() => null);
    if (!stats) {
      return res.status(404).json({ error: "File or directory not found" });
    }

    // Cancella fisicamente dal filesystem
    if (stats.isDirectory()) {
      await fs.promises.rm(filePathAbs, { recursive: true, force: true });
    } else {
      await fs.promises.unlink(filePathAbs);
    }

    // Cancella i metadata dal DB (ON DELETE CASCADE gestisce eventuali figli)
    await f.deleteFile(relPath);
    res.status(200).json({ message: "Deletion completed" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// PATCH /files/chmod?relPath=...&perm=755
router.patch("/chmod", async (req, res) => {
  try {
    const relPath = req.query.relPath;
    const perm = req.query.perm; // stringa ottale, es. "644"
    const filePathAbs = path.join(ROOT_DIR, relPath); // FIX: era undefined
    await fs.promises.chmod(filePathAbs, parseInt(perm, 8));
    await f.updatePermissions(relPath, perm);
    res.status(200).json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "chmod failed" });
  }
});

// PATCH /files/truncate?relPath=...&size=123
router.patch("/truncate", async (req, res) => {
  try {
    const relPath = req.query.relPath;
    const size = parseInt(req.query.size, 10);
    const filePathAbs = path.join(ROOT_DIR, relPath);
    await fs.promises.truncate(filePathAbs, size);
    const stats = await fs.promises.stat(filePathAbs);
    await f.updateFile({
      path: relPath,
      name: path.basename(filePathAbs),
      parent: path.dirname(relPath),
      is_dir: false,
      size: stats.size,
      mtime: Math.floor(stats.mtimeMs / 1000),
      permissions: undefined, // non toccare qui
    });
    res.status(200).json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "truncate failed" });
  }
});

// PATCH /files/utimes?relPath=...&atime=...&mtime=...
router.patch("/utimes", async (req, res) => {
  try {
    const relPath = req.query.relPath;
    const at = req.query.atime ? parseInt(req.query.atime, 10) : null; 
    const mt = req.query.mtime ? parseInt(req.query.mtime, 10) : null; 
    const filePathAbs = path.join(ROOT_DIR, relPath);

    // Se mancano, usa stat per tenere l'altro inalterato
    const stats = await fs.promises.stat(filePathAbs);
    const atime = at ? new Date(at * 1000) : stats.atime;
    const mtime = mt ? new Date(mt * 1000) : stats.mtime;
    await fs.promises.utimes(filePathAbs, atime, mtime);
    const stats2 = await fs.promises.stat(filePathAbs);
    await f.updateMtime(relPath, Math.floor(stats2.mtimeMs / 1000));
    res.status(200).json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "utimes failed" });
  }
});

export default router;
