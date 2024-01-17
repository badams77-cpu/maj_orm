package com.majorana.orm.MajORM.DBs;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.slf4j.Logger;
import com.majorana.orm.MajORM.Utils.MethodPrefixingLoggerFactory;
import com.majorana.orm.MajORM.DBs.DatabaseVariant;

import java.net.InetSocketAddress;

public class CassandraConnector {

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(CassandraConnector.class);
    private CqlSession session;

    private String message = "";

    public void connect(DBCreds creds){
        if (creds.getVariant()== DatabaseVariant.CASSANDRA){
           connectInner(creds);
        } else {
            message = "Not Cassandra";
        }
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void connectInner(DBCreds creds) {
        CqlSessionBuilder builder = CqlSession.builder();
        builder.addContactPoint(new InetSocketAddress(creds.getHostAddress(), creds.getPort()));
        builder.withLocalDatacenter( creds.getGroup());

        builder.withAuthCredentials(creds.getUsername(), creds.getUsername());

        session = builder.build();
    }

    public CqlSession getSession() {
        return this.session;
    }

    public void close() {
        session.close();
    }
}