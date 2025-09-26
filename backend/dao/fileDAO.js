import db from '../db/fileDB.js';

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
    this.updateFile = ({path, name, parent, is_dir, size, mtime, permissions}) => {
        const query = 'INSERT INTO files(path, parent, name, is_dir, size, mtime, permissions, version) VALUES(?, ?, ?, ?, ?, ?, ?, 1) ON CONFLICT(path) DO UPDATE SET size =?, mtime =?, version = version + 1'
        return new Promise((resolve, reject) => {
            db.run(query, [path, name, parent, is_dir, size, mtime, permissions], (err, row) => {
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
    this.createDirectory=({path, parent, name, is_dir, size, mtime, permissions})=>{
        const query = 'INSERT INTO files(path, parent, name, is_dir, size, mtime, permissions, version) VALUES (?, ?, ?, ?, ?, ?, ?, 1)'
        return new Promise((resolve, reject) => {
            db.run(query, [path, parent, name, is_dir, size, mtime, permissions], (err, row) =>{
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
    this.deleteFile=(path)=>{
        const query = 'DELETE FROM files WHERE path = ? OR path LIKE "path/%"'
        db.run(query, [path], (err, row) =>{
            if (err) {
                reject(err);
            }
            if (!row) {
                resolve({ error: 'File not found.' });
            } else {
                resolve();
            }
        });
    }

}