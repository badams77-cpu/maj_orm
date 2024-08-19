package Distiller.DBs;

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
