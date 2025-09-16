import db from '../db/fileDB.js';

export default function FileDAO() {
    this.getFilesByDirectory = (parent) => {
        return new Promise((resolve, reject) => {
            const query = 'SELECT * FROM files WHERE parent=?';
            db.all(query, [parent], (err, row) => {
                if (err) {
                    reject(err);
                }
                if (!row) {
                    resolve({ error: 'Directory not found.' });
                } else {
                    resolve();
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
    this.updateFile = (path, parent, name, is_dir, size, mtime, permissions,version) => {
        return new Promise((resolve, reject) => {
            const query = 'INSERT INTO files(path, parent, name, is_dir, size, mtime, permissions, version) VALUES(?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(path) DO UPDATE SET size =?, mtime =?, version = version + 1'
            db.run(query, [path, parent, name, is_dir, size, mtime, permissions,version], (err, row) => {
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
    this.createDirectory=(path, parent, name, is_dir, size, mtime, permissions,version)=>{
        const query = 'INSERT INTO files(path, parent, name, is_dir, size, mtime, permissions, version) VALUES (?, ?, ?, 1, 0, ?, ?, 1)'
        db.run(query, [path, parent, name, is_dir, size, mtime, permissions,version], (err, row) =>{
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