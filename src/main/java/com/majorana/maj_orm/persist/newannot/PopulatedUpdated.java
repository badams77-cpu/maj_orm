package com.majorana.maj_orm.persist.newannot;

/**
 * Annotation to automatic populated modification time on a field
 * when created
 */
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)

public @interface PopulatedUpdated {

}
