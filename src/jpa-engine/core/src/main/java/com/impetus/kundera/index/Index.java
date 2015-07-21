/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.index;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation interface for column
 * 
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

    /**
     * Max value of index column.
     * 
     * @return
     */
    public abstract int max() default Integer.MAX_VALUE;

    /**
     * Min value of index column.
     * 
     * @return
     */
    public abstract int min() default Integer.MIN_VALUE;

    /**
     * Name of index, if it is different that column name.
     * 
     * @return index name.
     */
    public abstract String indexName() default "";

    /**
     * Type of index.
     * 
     * @author Kuldeep.Mishra
     * 
     */
    public enum IndexType
    {
        normal, composite, unique
    }

}
