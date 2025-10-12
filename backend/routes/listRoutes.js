import express from 'express';
import FileDAO from '../dao/fileDAO.js';
import path from "path";

const router = express.Router();
const file=new FileDAO();

// GET /list/path
router.get('/', async(req, res) =>{
    try{
        const relPath= req.query.relPath;
        let dirname = path.basename(relPath);
        if (dirname === ''){
            dirname='.';
        }
        // console.log("Listing directory:", dirname);
        const files = await file.getFilesByDirectory(dirname);
        res.json(files);
    } catch(err){
        res.status(500).json({error: err.message});
    }
});

export default router;