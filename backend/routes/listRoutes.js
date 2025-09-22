import express from 'express';
import FileDAO from '../dao/fileDAO.js';

const router = express.Router();
const file=new FileDAO();

// GET /list/path
router.get('/:directory', async(req, res) =>{
    try{
        const {directory} = req.params.directory;
        const files = await file.getFileByPath(directory);
        if( files.length ===0 ){
            return res.status(404).json({error: 'No files found'});
        }
    } catch(err){
        res.status(500).json({error: err.message});
    }
});

export default router;