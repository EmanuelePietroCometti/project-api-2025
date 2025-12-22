import db from '../db/fileDB.js';
import p from "path";
import fs from "fs";

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
                    resolve({ error: 'Directory not found.' });
                } else {
                    resolve(row);
                }
            });
        });
    };

    this.createDirectory = async ({ path, parent, name, is_dir, size, mtime, permissions, nlink }) => {
        const parentPath = p.dirname(path);
        const parent_id = await this.getIdByPath(parentPath);
        const query = 'INSERT INTO files(path, parent_id,parent, name, is_dir, size, mtime, permissions, nlink, version) VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, 1)'
        return new Promise((resolve, reject) => {
            db.run(query, [path, parent_id, parent, name, is_dir, size, mtime, permissions, nlink], (err, row) => {
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


    this.getIdByPath = (path) => {
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
    // Aggiorna solo permissions
    this.updatePermissions = (path, permissions) => {
        const q = 'UPDATE files SET permissions=?, version=version+1 WHERE path=?';
        return new Promise((resolve, reject) => {
            db.run(q, [permissions, path], function (err) {
                if (err) return reject(err);
                resolve({ success: this.changes > 0 });
            });
        });
    };

    // Correggi UPSERT per includere permissions nell’UPDATE e con binding corretti
    this.updateFile = async ({ path, name, parent, is_dir, size, mtime, permissions, nlink }) => {
        const parentPath = p.dirname(path);
        const parent_id = await this.getIdByPath(parentPath);

        const q = `
        INSERT INTO files(path, parent_id, parent, name, is_dir, size, mtime, permissions, nlink, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(path) DO UPDATE SET
            size=excluded.size,
            mtime=excluded.mtime,
            permissions=COALESCE(excluded.permissions, files.permissions),
            nlink=COALESCE(excluded.nlink, files.nlink),
            version=files.version + 1
    `;

        return new Promise((resolve, reject) => {
            db.run(q, [path, parent_id, parent, name, is_dir, size, mtime, permissions ?? null, nlink ?? 1], function (err) {
                if (err) return reject(err);
                resolve({ success: this.changes >= 0 });
            });
        });
    };

    this.rename = async (oldPath, newPath) => {
        const newParentPath = p.dirname(newPath);
        const newParentId = await this.getIdByPath(newParentPath);
        const newName = p.basename(newPath);
        const query = 'UPDATE files SET path=?, parent_id=?, parent=?, name=?, version=version+1 WHERE path=?';
        return new Promise((resolve, reject) => {
            db.run(query, [newPath, newParentId, newParentPath, newName, oldPath], function (err) {
                if (err) return reject(err);
                if (this.changes === 0) {
                    resolve({ error: 'File not found.' });
                } else {
                    resolve({ success: true });
                }
            });
        });
    }

    this.syncMetadataFromDisk = async (relPath) => {
        // Corretto il riferimento da 'path.join' a 'p.join'
        const absPath = (relPath === '.' || relPath === '/')
            ? p.join(process.cwd(), "storage")
            : p.join(p.join(process.cwd(), "storage"), relPath);

        try {
            const stats = await fs.promises.stat(absPath);
            const query = `
                UPDATE files 
                SET size = ?, nlink = ?, mtime = ?, version = version + 1 
                WHERE path = ?
            `;

            return new Promise((resolve, reject) => {
                db.run(query, [
                    stats.size,
                    stats.nlink,
                    Math.floor(stats.mtimeMs / 1000),
                    relPath
                ], function (err) {
                    if (err) reject(err);
                    else resolve({ changes: this.changes });
                });
            });
        } catch (err) {
            console.error(`Error on metadata synchronization for ${relPath}:`, err);
        }
    };

    this.getNLinkByDirectory = async (path) => {
        return new Promise((resolve, reject) => {
            const query = 'SELECT nlink FROM files WHERE parent=?';
            db.all(query, [path], (err, rows) => {
                if (err) return reject(err);
                resolve(rows || []);
            });
        });
    }
}  