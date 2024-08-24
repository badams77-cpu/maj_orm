package com.com.majorana.ORM.ex1;

import com.majorana.ORM.BaseMajoranaEntity;
import com.majorana.entities.ex1.TableNames;
import jakarta.persistence.Column;



/**
 * Category - An example Entity to persist to a DB,
 * it uses the jakarta persistance @Column annotation to name the database table
 * for each non transient and non static field
 */

public class Category extends BaseMajoranaEntity {

	private static final String TABLE_NAME = TableNames.CATEGORIES;

	@Column(name="cid")
	private int cid;
	@Column(name="cname")
	private String cname;
	@Column(name="npages")
	private int npages;
	
	public final static String fields = ", cid, cname, npages";

	public static String getFields(){
		return getBaseFields()+", "+fields;
	}

	public String  getTableName(){
		return TABLE_NAME;
	}

	public int getCid(){ return cid; }
	public void setCid(int i){ cid=i; }
	
	public String getCname(){ return cname; }
	public void setCname(String s){ cname=s; }
	
	public int getNpages(){ return npages; }
	public void setNpage(int np){ npages=np; }
	
}
