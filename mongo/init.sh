#!/bin/sh

mongo -u ${MONGO_ADMIN} -p ${MONGO_ADMIN} localhost:27017/${MONGO_ADMIN} <<-EOF
    rs.initiate({
        _id: "rs0",
        members: [ { _id: 0, host: getHostName() + ":27017" } ]
    });
EOF

sleep 5