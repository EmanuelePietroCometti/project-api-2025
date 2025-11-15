import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const router = express.Router();

// Logica per trovare la ROOT_DIR (copiata da mkdirRoutes.js)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.join(__dirname, "..", "storage");

// GET /stats
router.get('/', async (req, res) => {
  try {
    // Chiama fs.statfs sul tuo storage
    const stats = await fs.promises.statfs(ROOT_DIR);

    // Converte i BigInt in stringhe per sicurezza nel JSON, 
    // ma u64 in Rust dovrebbe gestirli. In alternativa, invia come Number se sei sicuro che non superino 2^53.
    // Per compatibilità massima, usiamo la conversione in stringa e serde in Rust la parserà a u64.
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