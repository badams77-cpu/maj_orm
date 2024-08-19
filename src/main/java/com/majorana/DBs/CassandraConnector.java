package Distiller.DBs;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import Distiller.Utils.MethodPrefixingLoggerFactory;
import org.slf4j.Logger;

import java.net.InetSocketAddress;

public class CassandraConnector {

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(CassandraConnector.class);
    private CqlSession session;

    private String message = "";

    public void connect(DBCreds creds, CassandraState state){
        if (creds.getVariant()==DatabaseVariant.CASSANDRA){
            connectInner(creds, state);
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

    public void connectInner(DBCreds creds, CassandraState state) {
        if (state.isEnabbled()) {
            CqlSessionBuilder builder = CqlSession.builder();
            builder.addContactPoint(new InetSocketAddress(creds.getHostAddress(), creds.getPort()));
            builder.withLocalDatacenter(creds.getGroup());

            builder.withAuthCredentials(creds.getUsername(), creds.getUsername());

            session = builder.build();
        }
    }

    public CqlSession getSession() {
        return this.session;
    }

    public void close() {
        session.close();
    }
}