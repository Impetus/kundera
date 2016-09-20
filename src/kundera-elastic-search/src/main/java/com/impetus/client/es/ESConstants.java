/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.es;

/**
 * Constants for ES.
 * 
 * @author Amit Kumar
 */
public interface ESConstants
{
    /** The Constant dot. */
    public static final char DOT = '.';

    /** The Constant leftBracket. */
    public static final char LEFT_BRACKET = '(';

    /** The Constant rightBracket. */
    public static final char RIGHT_BRACKET = ')';

    /** The Constant asterisk. */
    public static final String ASTERISK = "*";

    /** The Constant percentage. */
    public static final String PERCENTAGE = "%";

    /** The Constant infinity. */
    public static final String INFINITY = "INFINITY";

    /** The Constant aggName. */
    public static final String AGGREGATION_NAME = "esAggs";

    /** The Constant groupBy. */
    public static final String GROUP_BY = "group by";

    /** The Constant topHits. */
    public static final String TOP_HITS = "top";

    /** The Constant DEFAULT. */
    public static final String DEFAULT = "DEFAULT";

    /** The Constant ES_REFRESH_INDEXES. */
    public static final String ES_REFRESH_INDEXES = "es.refresh.indexes";
    
    /** The Constant KUNDERA_ES_REFRESH_INDEXES. */
    public static final String KUNDERA_ES_REFRESH_INDEXES = "kundera.es.refresh.indexes";
}
