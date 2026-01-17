import express from 'express';
import FileDAO from '../dao/fileDAO.js';
import { ROOT_DIR } from '../index.js';

const router = express.Router();
const file = new FileDAO();

// GET /list/path
router.get('/', async (req, res) => {
    try {
        let dirname = req.query.relPath;
        if (dirname === '' || dirname === './storage') {
            dirname = '.';
        }
        if (dirname.startsWith('././')) {
            dirname = dirname.slice(2);
        }
        const files = await file.getFilesByDirectory(dirname);
        res.json(files);
        return;

    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

router.get("/updatedMetadata", async (req, res) => {
    try {
        let path = req.query.relPath;
        if (!path || path=="" || path== "./storage") {
            path = ".";
        }
        if (path.startsWith('././')) {
            path = path.slice(2);
        }
        const f = await file.getFileByPath(path);
        if (!f) {
            return res.status(404).json({ error: "Not found" });
        }
        res.json(f);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});


export default router;