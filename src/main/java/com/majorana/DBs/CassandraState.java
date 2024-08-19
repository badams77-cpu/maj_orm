package Majorana.DBs;

/**
 * CassandraState sets weather a cassandra connection with be tried by the ORM
 */

public class CassandraState {

    private boolean enabled;

    public CassandraState(boolean cassStat){
        enabled = cassStat;
    }

    public boolean isEnabbled(){
        return enabled;
    }


    public void setEnabled(boolean enabled){
        this.enabled = enabled;

    }
}
