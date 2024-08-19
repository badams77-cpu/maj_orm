package Distiller.DBs;

import Distiller.Utils.GenericUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.IsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

;

public class SmokDataSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(SmokDataSource.class);

    private DBCreds cred;

    // Set this config value to one of the values in com.zaxxer.hikari.util.IsolationLevel, eg TRANSACTION_READ_COMMITTED
    // Leave as blank for the database server default isolation level

    public SmokDataSource(DBCreds creds){
        this.cred = creds;
    }

    public DatabaseVariant getVariant(){
        return cred.getVariant();
    }

    public HikariDataSource getHikariDataSource() {

        LOGGER.info("Creating HikariDataSource with creds "+cred.toString());

        HikariConfig conf = new HikariConfig();

        String url = null;

        // Provide settings.
        // Ref: http://blog.zenika.com/2013/01/30/using-tomcat-jdbc-connection-pool-in-a-standalone-environment/

        LOGGER.info(String.format("SmokDataSource: using SSL settings useSSL=%s, verifyServerCertificate=%s, allowPublicKey=%s,",
                cred.isUseSSL(), cred.isVerifySSLCert(), cred.isAllowPublicKeyRetrieval()));


        switch(DatabaseVariant.getFromDescription(cred.getVariant().getDescription())){
            case MYSQL:
                url = String.format(
                        "jdbc:mysql://%s/%s?zeroDateTimeBehavior=convertToNull&useSSL=%s"+"" +
                                "&verifyServerCertificate=%s&allowPublicKeyRetrieval=%s",
                        cred.getHostAddress(),
                        cred.getRemoteDatabaseNameAtService(),
                        cred.isUseSSL(),
                        cred.isVerifySSLCert(),
                        cred.isAllowPublicKeyRetrieval()
                );
//                ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
                break;
            case SQL_SERVER:
                url = String.format(
                        "jdbc:sqlserver://%s;databaseName=%s;user=%s;password=%s;",cred,
                        cred.getName() , cred.getUsername() , cred.getPasswd(), cred.isVerifySSLCert());
                conf.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                break;
            case CASSANDRA:
                return null;

            default:
                throw new IllegalStateException("MPC_DB_VARIANT unknown value "+cred.getVariant());
        }

        conf.setJdbcUrl(url);

        if (!url.contains("user")) {
            conf.setUsername(cred.getUsername());
        }
        if (!url.contains("password")) {
            conf.setPassword(cred.getPasswd());
        }

        IsolationLevel isoLevel = null;
        if (!GenericUtils.isNullOrEmpty(cred.getIsolationLevel())) {
            try {
                isoLevel = IsolationLevel.valueOf(cred.getIsolationLevel());
                if (isoLevel.getLevelId() < IsolationLevel.TRANSACTION_READ_COMMITTED.getLevelId()) {
                    LOGGER.error("SmokDataSource: invalid transaction isolation level: " + cred.getIsolationLevel() + " - must be at least TRANSACTION_READ_COMMITTED, will use server default");
                    isoLevel = null;
                }
            } catch (Exception e) {
                LOGGER.warn("SmokDataSource: unknown transaction isolation level: " + cred.getIsolationLevel() + " , will use server default");
            }
        }
        if (isoLevel != null) {
            LOGGER.warn("SmokDataSource: setting isolation level: " + cred.getIsolationLevel());
            conf.setTransactionIsolation("" + isoLevel.getLevelId());
        } else {
            LOGGER.warn("SmokDataSource: using server default isolation level");
        }

        conf.setConnectionTestQuery("SELECT 1");

        HikariDataSource hds = new HikariDataSource(conf);

        return hds;

    }

}