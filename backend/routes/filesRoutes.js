import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import FileDAO from '../dao/fileDAO.js';


const router = express.Router();
const f = new FileDAO();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.join(__dirname, "..", "storage");

// GET /files/path
router.get('/:path', async (req, res) => {
    try {
        const relpath = req.params.path;
        const filePath = path.join(ROOT_DIR, relPath);
        const parentPath = path.dirname(filePath);

        // Controlla che la directory padre esista
        if (!fs.existsSync(parentPath)) {
            return res.status(400).json({ error: "Parent directory not found. Create the directory first." });
        }
        if (fs.is_dir) {
            return res.status(400).json({ error: "Path is referenced to a directory." });
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
router.put('/:path', async (req, res) => {
    try {
        const relPath = req.params.path;
        const filePath = path.join(ROOT_DIR, relPath);
        const parentPath = path.dirname(filePath);

        // Controlla che la directory padre esista
        if (!fs.existsSync(parentPath)) {
            return res.status(400).json({ error: "Parent directory not found. Create the directory first." });
        }

        // Scrivi file con streaming
        const writeStream = fs.createWriteStream(filePath);
        req.pipe(writeStream);

        writeStream.on("finish", async () => {
            const stats = await fs.promises.stat(filePath);

            // Aggiorna o inserisci metadata nel DB
            await f.updateFile({
                path: relPath,
                name: path.basename(relPath),
                parent: path.dirname(relPath),
                is_dir: false,
                size: stats.size,
                mtime: Math.floor(stats.mtimeMs / 1000),
                permissions: "0644",
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
router.delete('/:path', async (req, res) => {
    try {
        try {
            const relPath = req.params.path;
            const filePath = path.join(ROOT_DIR, relPath);

            // Controlla se esiste
            const stats = await fs.promises.stat(filePath).catch(() => null);
            if (!stats) {
                return res.status(404).json({ error: "File or directory not found" });
            }

            // Cancella fisicamente dal filesystem
            if (stats.isDirectory()) {
                await fs.promises.rm(filePath, { recursive: true, force: true });
            } else {
                await fs.promises.unlink(filePath);
            }

            // Cancella i metadata dal DB (ON DELETE CASCADE gestisce eventuali figli)
            await f.deleteFile(relPath);

            res.status(200).json({ message: "Deletion completed" });

        } catch (err) {
            console.error(err);
            res.status(500).json({ error: "Internal server error" });
        }
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

export default router;