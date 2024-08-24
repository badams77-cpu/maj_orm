package com.majorana.ORM_ACCESS;

public @interface EntityPackage {
    int minor() default 1;
    int major() default 1;
}