package com.majorana.maj_orm.entities.ex2;

import com.majorana.maj_orm.newannot.EntityPackage;


/**
 *  An Example persisted entity in a package directory with a higher version number
 *  if the ENV version is below 2 the entities in the directory will not be persisted
 */

@EntityPackage( major=2, minor=1)
public class TableNamesExtra {

    public static final String PRIVATE_FEEDS = "private_feeds";

}
