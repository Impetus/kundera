/**
 * 
 */
package com.impetus.kundera.newannotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Kuldeep Mishra
 * 
 */
@Target({ ElementType.TYPE, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Index
{

    /**
     * Column to index.
     * 
     * @return the string
     */
    public abstract String name();

    /**
     * Type of index.
     * 
     * @return the string
     */
    public abstract String type() default "";

    public abstract int max() default 0;

    public abstract int min() default 0;

}
