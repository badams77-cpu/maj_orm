package com.majorana.persist.newannot;

/**
 * An Annotation for an Entity in Maj ORM that is additional to the jakarta persistance,
 * it automatically populates a created or update database entry with the time of creation
 * or modficatioln
 */

    public @interface AutoPopTimestamp {

        boolean updated();
        boolean created();

    }
