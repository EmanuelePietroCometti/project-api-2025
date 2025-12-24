import express from 'express';
import FileDAO from '../dao/fileDAO.js';
import { ROOT_DIR } from '../index.js';

const router = express.Router();
const file = new FileDAO();

// GET /list/path
router.get('/', async (req, res) => {
    try {
        let dirname = req.query.relPath;
        if (dirname === '') {
            dirname = '.';
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
        if (!path || path === "") {
            path = ROOT_DIR;
        }
        const f = await file.getFileByPath(path);
        res.json(f);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});


export default router;