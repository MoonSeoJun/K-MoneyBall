#!/bin/sh

mongosh -u ${MONGO_ADMIN} -p ${MONGO_ADMIN} <<-EOF
    rs.initiate({
        _id: "rs0",
        members: [ { _id: 0, host: "mongo:27017" } ]
    });
EOF

sleep 5