package com.majorana.entities;

import jakarta.persistence.Column;

public class Category extends BaseDistillerEntity {

	private static final String TABLE_NAME = TableNames.CATEGORIES;

	@Column(name="cid")
	private int cid;
	@Column(name="cname")
	private String cname;
	@Column(name="npages")
	private int npages;
	
	public final static String fields = ", cid, cname, npages";

	public String getFields(){
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
