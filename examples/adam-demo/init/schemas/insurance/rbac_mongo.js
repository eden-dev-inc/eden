// MongoDB RBAC for Insurance vertical (database: incidents)

db = db.getSiblingDB('admin');

if (!db.getUser('reader')) {
    db.createUser({
        user: 'reader',
        pwd: 'reader_pass',
        roles: [
            { role: 'read', db: 'incidents' }
        ]
    });
}

if (!db.getUser('writer')) {
    db.createUser({
        user: 'writer',
        pwd: 'writer_pass',
        roles: [
            { role: 'readWrite', db: 'incidents' }
        ]
    });
}
