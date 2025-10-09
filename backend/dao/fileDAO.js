import db from '../db/fileDB.js';
import p from "path";

export default function FileDAO() {
    this.getFilesByDirectory = (parent) => {
        return new Promise((resolve, reject) => {
            const query = 'SELECT * FROM files WHERE parent=?';
            db.all(query, [parent], (err, rows) => {
                if (err) {
                    reject(err);
                }
                if (!rows) {
                    resolve({ error: 'Directory not found.' });
                } else {
                    resolve(rows);
                }
            });
        });
    }
    this.getFileByPath = (path) => {
        return new Promise((resolve, reject) => {
            const query = 'SELECT * FROM files WHERE path=?';
            db.get(query, [path], (err, row) => {
                if (err) {
                    reject(err);
                }
                if (!row) {
                    resolve({ error: 'File not found.' });
                } else {
                    resolve();
                }
            });
        });
    }
    this.updateFile = async ({path, name, parent, is_dir, size, mtime, permissions}) => {
        const parentPath= p.dirname(path);
        const parent_id= await this.getIdByPath(parentPath);
        console.log(parent_id);
        const query = 'INSERT INTO files(path,parent_id, parent, name, is_dir, size, mtime, permissions, version) VALUES(?,? ,?, ?, ?, ?, ?, ?, 1) ON CONFLICT(path) DO UPDATE SET size =?, mtime =?, version = version + 1'
        return new Promise((resolve, reject) => {
            db.run(query, [path,parent_id, parent, name, is_dir, size, mtime, permissions], (err, row) => {
                if (err) {
                    reject(err);
                }
                if (!row) {
                    resolve({ error: 'File not found.' });
                } else {
                    resolve();
                }
            })
        });
    }
    this.createDirectory=async ({path, parent, name, is_dir, size, mtime, permissions})=>{
        const parentPath= p.dirname(path);
        const parent_id= await this.getIdByPath(parentPath);
        const query = 'INSERT INTO files(path, parent_id,parent, name, is_dir, size, mtime, permissions, version) VALUES (?,?, ?, ?, ?, ?, ?, ?, 1)'
        return new Promise((resolve, reject) => {
            db.run(query, [path, parent_id,parent, name, is_dir, size, mtime, permissions], (err, row) =>{
                if (err) {
                    reject(err);
                }
                if (!row) {
                    resolve({ error: 'File not found.' });
                } else {
                    resolve();
                }
            });
        });
    }
    this.deleteFile = async (path) => {
    const query = 'DELETE FROM files WHERE path = ?';
    return new Promise((resolve, reject) => {
        db.run(query, [path], function (err) {
            if (err) return reject(err);
            if (this.changes === 0) {
                resolve({ error: 'File not found.' });
            } else {
                resolve({ success: true });
            }
        });
    });
};


    this.getIdByPath=(path)=>{
        return new Promise((resolve, reject) => {
            const query = 'SELECT * FROM files WHERE path=?';
            db.get(query, [path], (err, row) => {
                
                 if (err) return reject(err);
                if (!row) return resolve(null); 
                //console.log(row.id);
                resolve(row.id);
            });
        });
    }

}