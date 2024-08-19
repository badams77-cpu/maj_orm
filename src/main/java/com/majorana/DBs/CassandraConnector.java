package Majorana.DBs;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import Majorana.Utils.MethodPrefixingLoggerFactory;
import org.slf4j.Logger;

import java.net.InetSocketAddress;

/**
 *   The Cassandra Connector - Contected the Maj DB ORM to an Optional Number of Cassandra Databases
 *
 */


public class CassandraConnector {

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(CassandraConnector.class);
    private CqlSession session;

    private String message = "";

    /**
     *  The connect method tried the Database Credentials to connected to a Cassamdra Datanbase iff
     *  the CassandraState is enabled
     */
    public void connect(DBCreds creds, CassandraState state){
        if (creds.getVariant()==DatabaseVariant.CASSANDRA){
            connectInner(creds, state);
        } else {
            message = "Not Cassandra";
        }
    }

    /**
     * String  getMessage()
     *
     * @return returns the message from the last commection try
     */

    public String getMessage() {
        return message;
    }

    /**
     *  void setMessage(String message)
     *
     *  Set the connection message, generally used intertnally only
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     *  public void connectInner(DBCred crefds, CassandraState state)
     *
     *   Performs the connection and creates a cassandra session if only the cass state is enabled
     *
     */

    public void connectInner(DBCreds creds, CassandraState state) {
        if (state.isEnabbled()) {
            CqlSessionBuilder builder = CqlSession.builder();
            builder.addContactPoint(new InetSocketAddress(creds.getHostAddress(), creds.getPort()));
            builder.withLocalDatacenter(creds.getGroup());

            builder.withAuthCredentials(creds.getUsername(), creds.getUsername());

            session = builder.build();
        }
    }

    /**
     *  CqlSession getSession
     *
     * @return returns the cassandra CqlSession if availahle, it may be null
     */

    public CqlSession getSession() {
        return this.session;
    }

    /**
     *  void close()
     *
     * closes the cassandra session
     */

    public void close() {
        session.close();
    }
}