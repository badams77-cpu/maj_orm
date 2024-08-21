package com.majorana.entities.ex1;

import com.majorana.persist.newannot.Updateable;
import jakarta.persistence.Column;
import main.Majorana.enum_const.ACCESS_LEVEL;


/**
 * Feed it uses the jakarta persistance @Column annotation to name the database table
 * for each non transient and non static field
 */

public class Feed extends BaseMajoranaEntity {

  private static final String TABLE_NAME = TableNames.FEEDS;

  @Column(name="feedid")
  private int feedid;
  @Updateable
  @Column(name="feedname")
  private String feedname;
  @Updateable
  @Column(name="owner")
  private int owner;
  @Updateable
  @Column(name="last_access")
  private long last_access;
  @Column(name="last_updated")
  @Updateable
  private long last_updated;
  @Updateable
  @Column(name="expiry_time")
  private int expiry_time;
  @Updateable
  @Column(name="regularality")
  private int regularality;
  @Updateable
  @Column(name="no_items")
  private int no_items;
  @Updateable
  @Column(name="no_scanned")
  private int no_scanned;
  @Updateable
  @Column(name="accessname")
  private String accessname;


  @Updateable
  @Column(name="accessLevel")
  private ACCESS_LEVEL accessLevel;
  @Updateable
  @Column(name="password")
  private String password;
  @Updateable
  @Column(name="priority")
  private int priority;
  @Updateable
  @Column(name="nextBuild")
  private long nextBuild;
  @Updateable
  @Column(name="popularity")
  private int popularity;
  @Updateable
  @Column(name="category")
  private String category;
  @Updateable
  @Column(name="filterType")
  private String filterType;

  public final static String fields = ", feedid,feedname,owner,last_access,last_updated, expiry_time, regularality, no_items, no_scanned, accessname, password, priority, nextBuild, popularity, category, filterType";

  public String getFields(){
    return getBaseFields()+fields;
  }

  public String  getTableName(){
    return TABLE_NAME;
  }

  public Feed(){}

  public ACCESS_LEVEL getAccessLevel() {
    return accessLevel;
  }

  public void setAccessLevel(ACCESS_LEVEL accessLevel) {
    this.accessLevel = accessLevel;
  }

  public int getFeedid(){ return feedid; }
  public void setFeedid(int i){ feedid = i; }

  public String getFeedname(){ return feedname; }
  public void setFeedname(String i){ feedname = i; }

  public int getOwner(){ return owner; }
  public void setOwner(int ow){ owner = ow; }

  public long getLast_accessi

          (){ return last_access; }
  public void setLast_access(long lu){ last_access = lu; }

  public long getLast_updated(){ return last_updated; }
  public void setLast_updated(long lu){ last_updated = lu; }

  public int getExpiry_time(){ return expiry_time; }
  public void setExpiry_time(int et){  expiry_time = et; }

  public int getRegularality(){ return regularality; }
  public void setRegularality(int reg){ regularality = reg; }

  public int getNo_items(){ return no_items; }
  public void setNo_items(int it){ no_items  = it; }

  public int getNo_scanned(){ return no_scanned; }
  public void setNo_scanned(int ns ){ no_scanned = ns; }

  public String getAccessname(){ return accessname; }
  public void setAccessname(String sa){ accessname = sa; }

  public String getPassword(){ return password; }
  public void setPassword(String s){ password =s; }

  public int getPriority(){ return priority; }
  public void setPriority(int s){ priority=s; }
  
  public long getNextBuild(){ return nextBuild; }
  public void setNextBuild(long s){ nextBuild=s; }
  
  public int getPopularity(){ return popularity; }
  public void setPopularity(int i){ popularity=i; }
  
  public String getCategory(){ return category; }
  public void setCategory(String s){ category=s; }
  
  public String getFilterType(){ return filterType; }
  public void setFilterType(String s){ filterType=s; }
  
} 



