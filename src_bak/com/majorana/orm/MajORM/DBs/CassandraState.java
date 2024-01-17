package com.majorana.orm.MajORM.DBs;

public class CassandraState {

    private boolean enabled;

    public CassandraState(boolean enabled){
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
