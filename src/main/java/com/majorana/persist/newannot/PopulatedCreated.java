package com.majorana.persist.newannot;

/**
 * Annotation to automatical populate create time on a field in an entity
 */
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)

public @interface PopulatedCreated {

}
