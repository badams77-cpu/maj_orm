
package com.majorana.entities.ex1;

import com.majorana.persist.newannot.Updateable;
import jakarta.persistence.Column;
import main.Majorana.enum_const.ACCESS_LEVEL;

/**
 * An example user class for persistance
 */

public class User extends BaseMajoranaEntity {

  private static final String TABLE_NAME = TableNames.USERS;

  /*
  Matches a row in the User table of the database
*/

  @Column(name="uid")
  private int uid;
  @Updateable
  @Column(name="username")
  private String username;
  @Updateable
  @Column(name="membertype")
  private String membertype;
  @Updateable
  @Column(name="email")
  private String email;
  @Updateable
  @Column(name="forename")
  private String forename;
  @Updateable
  @Column(name="surname")
  private String surname;

  @Updateable
  @Column(name="accessLevel")
  private ACCESS_LEVEL accessLevel;

  public final static String fields=", uid, username, membertype, email, forename, surname";


  public String getFields(){
    return getBaseFields()+fields;
  }

  public String  getTableName(){
    return TABLE_NAME;
  }

  public User(){
  }

  public void copyFrom(User other){
    uid=other.uid; username=other.username; email=other.email;
    forename = other.forename; surname = other.surname; 
  }

  public ACCESS_LEVEL getAccessLevel() {
    return accessLevel;
  }

  public void setAccessLevel(ACCESS_LEVEL accessLevel) {
    this.accessLevel = accessLevel;
  }

  public void setUid(int i){ uid=i; }
  public int getUid(){ return uid; }

  public void setUsername(String name){ this.username = name; }
  public String getUsername(){ return username; }
  
  public void setMembertype(String mt){ this.membertype = mt; }
  public String getMembertype(){ return membertype; }

  public void setEmail(String email){ this.email = email; }
  public String getEmail(){ return email; }
 
  public void setForename(String name){ this.forename = name; }
  public String getForename(){ return forename; }

  public void setSurname(String name){ this.surname = name; }
  public String getSurname(){ return surname; }
  
}
