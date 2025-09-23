import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import FileDAO from '../dao/fileDAO.js';


const f = new FileDAO();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const router = express.Router();
const ROOT_DIR = path.join(__dirname, "..", "storage");

// POST /mkdir/path
router.post("/:path", async (req, res) => {
  try {
    const relPath = req.params.path;
    const dirPath = path.join(ROOT_DIR, relPath);
    const parentPath = path.dirname(dirPath);

    // Controlla se la directory padre esiste
    if (!fs.existsSync(parentPath)) {
      return res.status(400).json({ error: "Parent directory not found" });
    }

    // Controlla se la directory esiste già
    if (fs.existsSync(dirPath)) {
      return res.status(409).json({ error: "Directory already exist" });
    }

    // Crea la directory fisica
    await fs.promises.mkdir(dirPath);
    console.log({
      relPath,
      basename: path.basename(relPath),
      parentPath
    });

    // Inserisci nel DB i metadata
    await f.updateFile({
      path: relPath,
      name: path.basename(relPath),
      parent: parentPath,
      is_dir: true,
      size: 0,
      mtime: Math.floor(Date.now() / 1000),
      permissions: "755",
    });

    res.status(201).json({ message: "Directory successfully created" });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;