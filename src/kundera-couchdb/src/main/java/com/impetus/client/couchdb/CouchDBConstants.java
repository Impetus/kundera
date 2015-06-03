/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.couchdb;

/**
 * Constant Class for CouchDB.
 * 
 * @author Kuldeep Mishra
 * 
 */
public interface CouchDBConstants
{

    /** The Constant PROTOCOL. */
    public static final String PROTOCOL = "http";

    /** The Constant URL_SEPARATOR. */
    public static final String URL_SEPARATOR = "/";

    /** The Constant DESIGN. */
    public static final String DESIGN = "_design" + URL_SEPARATOR;

    /** The Constant VIEW. */
    public static final String VIEW = URL_SEPARATOR + "_view" + URL_SEPARATOR;

    /** The Constant LANGUAGE. */
    public static final String LANGUAGE = "javascript";

    /** The Constant LINE_SEP. */
    public static final String LINE_SEP = System.getProperty("line.separator");

    /** The Constant FIELDS. */
    public static final String FIELDS = "fields";

    /** The Constant COUNT. */
    public static final String COUNT = "count";

    /** The Constant ALL. */
    public static final String ALL = "ALL";

    /** The Constant SUM. */
    public static final String SUM = "sum";

    /** The Constant MAX. */
    public static final String MAX = "max";

    /** The Constant MIN. */
    public static final String MIN = "min";

    /** The Constant AVG. */
    public static final String AVG = "avg";

    /** The Constant ENTITYNAME. */
    public static final String ENTITYNAME = "ENTITYNAME";

    /** The Constant AGGREGATIONS. */
    public static final String AGGREGATIONS = "aggregations";
}
