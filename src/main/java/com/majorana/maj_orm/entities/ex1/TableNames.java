package com.majorana.maj_orm.entities.ex1;

import com.majorana.maj_orm.persist.newannot.EntityPackage;

/**
 *  An example table of names of the tables each entity is persisted too
 *
 *  NOte the entityversion annotation, the Environment may spefic a version
 *  below the version list in the direcory, and if so the entire directory
 *  will be ignored for persistance, enabled rolling back DB changes without
 *  changing the code
 *
 */


@EntityPackage(major=1,minor=0)
public class TableNames {

    public static final String NO_TABLE = null;

    public static final String ARTICLES = "articles";

    public static final String CATEGORIES = "categories";

    public static final String FEEDS = "feeds";

    public static final String EXPIRY_TIME = "junkusers";

    public static final String USERS = "users";


}
