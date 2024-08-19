package Distiller.DBs;

import java.util.Map;

import Distiller.Utils.MethodPrefixingLoggerFactory;

import org.slf4j.Logger;

/**
 * DBCreds specifies a connection to a database to try, multiple DNCreds may specify
 * multiple connectinns.
 */

public class DBCreds {


    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(DBCreds.class);

    private static final String ENV_VAR_NAME = "name";
    private static final String ENV_VAR_ISOLATION_LEVEL = "isolationLevel";
    private static final String ENV_VAR_DB_VARIANT = "dbVariant";
    private static final String ENV_VAR_HOST_ADDRESS = "hostAddress";
    private static final String ENV_VAR_PORT = "port";
    private static final String ENV_VAR_USERNAME = "username";
    private static final String ENV_VAR_PASSWD = "password";
    private static final String ENV_VAR_REMOTE_DATABASE_NAME_AT_SERVICE = "remoteDatabaseNameAtService";
    private static final String ENV_VAR_IS_ALLOWED_PUBLIC_KEY = "allowedPublicKey";
    private static final String ENV_VAR_PRIORITY = "priority";
    private static final String ENV_VAR_GROUP = "group";
    private static final String ENV_VAR_USE_SSL = "useSSL";
    private static final String ENV_VAR_VERIFY_SSL_CERT = "verifySSL";

    private static final String[] ENV_VARS = {
                ENV_VAR_NAME,
                ENV_VAR_ISOLATION_LEVEL,
                ENV_VAR_DB_VARIANT,
                ENV_VAR_HOST_ADDRESS,
                ENV_VAR_PORT,
                ENV_VAR_USERNAME,
                ENV_VAR_PASSWD,
            ENV_VAR_REMOTE_DATABASE_NAME_AT_SERVICE,
                ENV_VAR_PRIORITY,
                ENV_VAR_GROUP,
                ENV_VAR_USE_SSL,
                ENV_VAR_VERIFY_SSL_CERT,
                ENV_VAR_IS_ALLOWED_PUBLIC_KEY
    };


    private final SmokDatasourceName name;
    private final DatabaseVariant variant;
    private final String hostAddress;
    private final int port;
    private final String username;
    private final String passwd;
    private final String remoteDatabaseNameAtService;
    private final int priority;
    private final String group;
    private final boolean useSSL;
    private final boolean verifySSLCert;
    private final String isolationLevel;
    private final boolean allowPublicKeyRetrieval;

    public DBCreds(){
        this.name = new SmokDatasourceName("");
        this.variant = DatabaseVariant.NONE;
        this.hostAddress = "127.0.0.1";
        this.port = 3303;
        this.priority =0;
        this.remoteDatabaseNameAtService = "";
        this.username = "";
        this.passwd ="";
        this.group = "";
        this.isolationLevel = "";
        this.useSSL = false;
        this.verifySSLCert = false;
        this.allowPublicKeyRetrieval = false;
    }

    public DBCreds(SmokDatasourceName name, String
            group, int  priorty, DatabaseVariant variant, String remoteDatabaseNameAtService,
                   String hostAddress, int port, String username, String passwd, boolean useSSL, boolean verifySSLCert,
            String isolationLevel, boolean allowPublicKeyRetrieval)
    {
        this.isolationLevel = isolationLevel;
        this.name = name;
        this.variant = variant;
        this.hostAddress = hostAddress;
        this.port = port;
        this.priority =priorty;
        this.remoteDatabaseNameAtService = remoteDatabaseNameAtService;
        this.username = username;
        this.passwd = passwd;
        this.group = group;
        this.useSSL = useSSL;
        this.verifySSLCert = verifySSLCert;
        this.allowPublicKeyRetrieval = allowPublicKeyRetrieval;
    }

    DBCreds(Map<String, String> cred)
    {
        LOGGER.warn("Creating DBCreds from prop map : ",
                Distiller.Utils.MapDumpHelper.dump( cred, "\n",", ") );
        this.isolationLevel = cred.getOrDefault(ENV_VAR_ISOLATION_LEVEL,"");
        this.name = new SmokDatasourceName(cred.getOrDefault(ENV_VAR_NAME,""));
        this.variant = DatabaseVariant.getFromDescription(cred.getOrDefault(ENV_VAR_DB_VARIANT,""));
        this.hostAddress = cred.getOrDefault(ENV_VAR_HOST_ADDRESS,"");
        this.port = Integer.parseInt(cred.getOrDefault(ENV_VAR_PORT, "0"));
        this.priority = Integer.parseInt(cred.getOrDefault(ENV_VAR_PRIORITY,"0"));
        this.remoteDatabaseNameAtService = cred.getOrDefault(ENV_VAR_REMOTE_DATABASE_NAME_AT_SERVICE,"" );
        this.username = cred.getOrDefault(ENV_VAR_USERNAME,"");
        this.passwd = cred.getOrDefault(ENV_VAR_PASSWD,"");
        this.group = cred.getOrDefault(ENV_VAR_GROUP,"");
        this.useSSL = Boolean.parseBoolean(cred.getOrDefault(ENV_VAR_USE_SSL, ""));
        this.verifySSLCert = Boolean.parseBoolean(cred.getOrDefault(ENV_VAR_VERIFY_SSL_CERT, ""));
        this.allowPublicKeyRetrieval =  Boolean.parseBoolean(cred.getOrDefault(ENV_VAR_IS_ALLOWED_PUBLIC_KEY, "true"));
    }

    public static String[] getCredFields(){
        return ENV_VARS;
    }

    public boolean isAllowPublicKeyRetrieval(){
        return  allowPublicKeyRetrieval;
    }

    public DatabaseVariant getVariant() {
        return variant;
    }

    public String getIsolationLevel() {
        return isolationLevel;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPasswd() {
        return passwd;
    }

    public boolean isVerifySSLCert() {
        return verifySSLCert;
    }

    public String getGroup() {
        return group;
    }

    public SmokDatasourceName getName() {
        return name;
    }

    public String getRemoteDatabaseNameAtService() {
        return remoteDatabaseNameAtService;
    }

    public int getPriority() {
        return priority;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "DBCreds{" +
                "name=" + name +
                ", variant=" + variant +
                ", hostAddress='" + hostAddress + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", passwd='" + passwd.replaceAll(".","X") + '\'' +
                ", remoteDatabaseNameAtService='" + remoteDatabaseNameAtService + '\'' +
                ", priority=" + priority +
                ", group='" + group + '\'' +
                ", useSSL=" + useSSL +
                ", verifySSLCert=" + verifySSLCert +
                ", isolationLevel='" + isolationLevel + '\'' +
                ", allowPublicKeyRetrieval=" + allowPublicKeyRetrieval +
                '}';
    }
}
