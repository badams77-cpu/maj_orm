package com.majorana.entities;

import Majorana.ORM_ACCESS.EntityPackage;

/**
 * Specifies the version of all the entities in the same package and directory
 * the  entities will not be read or written to the db if the version here is higher than
 * the environment version number
 */


@EntityPackage(major=1, minor=1)
public class DirEntityVersion {
}
