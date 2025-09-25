import express from 'express';
import FileDAO from '../dao/fileDAO.js';

const router = express.Router();
const file=new FileDAO();

// GET /list/path
router.get('/:directory', async(req, res) =>{
    try{
        const directory = req.params.directory;
        const files = await file.getFilesByDirectory(directory);
        res.json(files);
    } catch(err){
        res.status(500).json({error: err.message});
    }
});

export default router;