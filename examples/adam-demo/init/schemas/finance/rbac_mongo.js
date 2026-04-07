// MongoDB RBAC for Finance vertical (database: compliance)

db = db.getSiblingDB('admin');

if (!db.getUser('reader')) {
    db.createUser({
        user: 'reader',
        pwd: 'reader_pass',
        roles: [
            { role: 'read', db: 'compliance' }
        ]
    });
}

if (!db.getUser('writer')) {
    db.createUser({
        user: 'writer',
        pwd: 'writer_pass',
        roles: [
            { role: 'readWrite', db: 'compliance' }
        ]
    });
}
