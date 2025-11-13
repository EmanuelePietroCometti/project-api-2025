import express from 'express';
import FileDAO from '../dao/fileDAO.js';
import path from "path";
import { dir } from 'console';

const router = express.Router();
const file=new FileDAO();

// GET /list/path
router.get('/', async(req, res) =>{
    try{
        let dirname = req.query.relPath;
        if (dirname === ''){
            dirname='.';
        }
        const files = await file.getFilesByDirectory(dirname);
        res.json(files);
        return;
        
    } catch(err){
        res.status(500).json({error: err.message});
    }
});

export default router;