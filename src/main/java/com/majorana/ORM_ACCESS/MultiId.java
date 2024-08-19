package com.majorana.ORM_ACCESS;

import java.util.UUID;

public class MultiId {

    private int id = 0;
    private UUID uuid = null;

    public MultiId(){

    }

    public MultiId(int id){
        this.id = id;
        this.uuid = null;
    }

    public MultiId(UUID uuid){
        this.id = 0;
        this.uuid = uuid;
    }

    public boolean hasAnyId(){
        return id!=0 || uuid!=null;
    }

    public boolean hasBoth(){
        return id!=0 && uuid!=null;
    }

    public boolean hasUuid(){
        return uuid !=null;
    }

    public boolean hasId(){
        return id!=0;
    }

    public int getId() {
        return id;
    }

    public UUID getUUID(){
        return uuid;
    }
}
