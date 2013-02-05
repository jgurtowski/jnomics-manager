package edu.cshl.schatz.jnomics.manager.client.ann;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: james
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Flag {
    public String group() default "";
    public String description() default "";
    public String shortForm();
    public String longForm();
}
