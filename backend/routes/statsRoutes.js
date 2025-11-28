import express from 'express';
import fs from 'fs';
import { ROOT_DIR } from '../index.js';

const router = express.Router();

// GET /stats
router.get('/', async (req, res) => {
  try {
    const stats = await fs.promises.statfs(ROOT_DIR);
    const response = {
      bsize: stats.bsize.toString(),
      blocks: stats.blocks.toString(),
      bfree: stats.bfree.toString(),
      bavail: stats.bavail.toString(),
      files: stats.files.toString(),
      ffree: stats.ffree.toString(),
    };

    res.json(response);

  } catch (err) {
    console.error("Errore in statfs:", err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;