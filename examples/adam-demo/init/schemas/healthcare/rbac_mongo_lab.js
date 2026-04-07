// MongoDB RBAC for Healthcare vertical (database: laboratory)

db = db.getSiblingDB('admin');

if (!db.getUser('reader')) {
    db.createUser({
        user: 'reader',
        pwd: 'reader_pass',
        roles: [
            { role: 'read', db: 'laboratory' }
        ]
    });
}

if (!db.getUser('writer')) {
    db.createUser({
        user: 'writer',
        pwd: 'writer_pass',
        roles: [
            { role: 'readWrite', db: 'laboratory' }
        ]
    });
}
