package com.majorana.entities.ex1;

import jakarta.persistence.Column;

/**
 * Article - An example Entity to persist to a DB,
 * it uses the jakarta persistance @Column annotation to name the database table
 * for each non transient and non static field
 */


public class Article extends BaseMajoranaEntity {

	private static final String TABLE_NAME = TableNames.ARTICLES;

	@Column(name="arid")
	private int arid;
	@Column(name="feedid")
	private int feedid;
	@Column(name="title")
	private String title;
	@Column(name="url")
	private String url;
	@Column(name="code")
	private String code;
	@Column(name="imageURL")
	private String imageURL;
	@Column(name="imageWidth")
	private int imageWidth;
	@Column(name="imageHeight")
	private int imageHeight;
	@Column(name="datastamp")
	private int datestamp;
	@Column(name="score")
	private float score;
	@Column(name="description")
	private String description;

	public static final String fields=", arid,feedid,title,url,code,imageURL,imageHeight,imageWidth,datestamp,score,description";

	public String  getTableName(){
		return TABLE_NAME;
	}

	public String getFields(){
		return getBaseFields()+", "+fields;
	}

	public int getArid(){ return arid; }
	public int getFeedid(){ return feedid; }
	public String getTitle(){ return title; }
	public String getURL(){ return url; }
	public String getCode(){ return code; }
	public String getImageURL(){ return imageURL; }
	public int getImageHeight(){ return imageHeight; }
	public int getImageWidth(){ return imageWidth; }
	public int getDatestamp(){ return datestamp; }
	public float getScore(){ return score; }
	public String getDescription(){ return description; }
	
	public void setArid(int arid){ this.arid=arid; }
	public void setFeedid(int feedid){ this.feedid=feedid; }
	public void setTitle(String title){ this.title=title; }
	public void setURL(String url){ this.url = url; }
	public void setCode(String code){ this.code = code; }
	public void setImageURL( String imageURL){ this.imageURL = imageURL; }
	public void setImageHeight(int h){ this.imageHeight = h; }
	public void setImageWidth(int w){ this.imageWidth = w; }
	public void setDatestamp(int date){ this.datestamp=date; }
	public void setScore( float score){ this.score = score; }
	public void setDescription(String description){
		if (description!=null && description.length()>800){ 
			this.description = description.substring(0,800);
		} else {
  		  this.description=description; 
		}
	}
	
	public String insertSQL(){
		return "insert into articles (feedid,title,url,code,imageURL,imageHeight,imageWidth,datestamp,score,description) values ("+
		    getFeedid()+",\""+getTitle()+"\",\""+getURL()+"\",\""+getCode()+"\",\""+getImageURL()+"\","+getImageHeight()+","+
		    getImageWidth()+","+getDatestamp()+","+getScore()+",\""+getDescription()+"\");";	
	}
	
}
