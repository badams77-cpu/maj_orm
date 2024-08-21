package com.majorana.entities.ex1;

import com.majorana.persist.newannot.Updateable;
import jakarta.persistence.Column;

/**
 * An example user persistance entity
 *
 */

public class UserEx extends BaseMajoranaEntity {

	private static final String TABLE_NAME = TableNames.NO_TABLE;

	/*
	  Matches a row in the User table of the database
	*/
	  public final static String fields = "uid,username,membertype,email,forename,surname,currentFeed";

	@Column(name="uid")
	  protected int uid;
	@Updateable
	@Column(name="username")
	  protected String username;
	@Updateable
	@Column(name="membertype")
	  protected String membertype;
	@Updateable
	@Column(name="email")
	  protected String email;
	@Updateable
	@Column(name="forename")
	  protected String forename;
	@Updateable
	@Column(name="surname")
	  protected String surname;
	@Updateable
	@Column(name="currentFeed")
      protected int currentFeed;
	@Updateable
	@Column(name="feedName")
      protected String feedName;

	@Updateable
	@Column(name="accessLevel")
	  protected int accessLevel;

	public String getFields(){
		return getBaseFields()+fields;
	}

	public UserEx(){

	}

	public String  getTableName(){
		return TABLE_NAME;
	}


	  public void copyFrom(UserEx other){
	    uid=other.uid; username=other.username; email=other.email;
	    forename = other.forename; surname = other.surname; 
	    currentFeed = other.currentFeed;
		accessLevel = other.accessLevel;
	  }

	  public void copyFrom(User other) {
		  uid = other.getUid();
		  username = other.getUsername();
		  email = other.getEmail();
		  forename = other.getForename();
		  surname = other.getSurname();
		  accessLevel = other.getAccessLevel().getLevelNum();
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
	  
	  public void setCurrentFeed(int currentFeed){ this.currentFeed= currentFeed; } 
	  public int getCurrentFeed(){ return currentFeed; }
	
	  public void setFeedName(String feedname){ feedName = feedname; }
	  public String getFeedName(){ return feedName; }
	  
	
}
