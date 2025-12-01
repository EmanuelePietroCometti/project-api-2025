import express from 'express';
import fs from 'fs';
import path from 'path';
import FileDAO from '../dao/fileDAO.js';
import { ROOT_DIR, backendChanges } from '../index.js';


const f = new FileDAO();
const router = express.Router();

// POST /mkdir/path
router.post("/", async (req, res) => {
  try {
    const relPath = req.query.relPath;
    const dirPath = path.join(ROOT_DIR, relPath);
    const parentPathAbs = path.dirname(dirPath);
    const parentDirName = path.dirname(relPath);
    const name = path.basename(dirPath);
    backendChanges.add(dirPath);
    console.log("dirPath in mkdirRoutes:", dirPath);
    
    if (!fs.existsSync(parentPathAbs) && parentPathAbs !== ROOT_DIR) {
      return res.status(400).json({ error: "Parent directory not found" });
    } 
    if (!fs.existsSync(parentPathAbs) && parentPathAbs === ROOT_DIR) {
      await fs.promises.mkdir(parentPathAbs);
    }

    if (fs.existsSync(dirPath)) {
      return res.status(409).json({ error: "Directory already exist" });
    }

    await fs.promises.mkdir(dirPath);
    const stats = await fs.promises.stat(dirPath);
    const permissions = (stats.mode & 0o777).toString(8);
    
    await f.createDirectory({
      path: relPath,
      parent: parentDirName,
      name: name,
      is_dir: true,
      size: stats.size,
      mtime: Math.floor(stats.mtimeMs / 1000),
      permissions
    });

    res.status(201).json({ message: "Directory successfully created" });

  } catch (err) {
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;