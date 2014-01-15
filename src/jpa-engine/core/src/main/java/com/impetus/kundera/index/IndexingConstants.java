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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class IndexingConstants.
 * 
 * @author vivek.mishra
 */
public final class IndexingConstants
{

    // persistent-unit-name

    /** The Constant UUID. */
    private static final long UUID = 6077004083174677888L;

    /** The Constant DELIMETER. */
    public static final String DELIMETER = "~";

    /** The Constant ENTITY_ID_FIELD. */
    public static final String ENTITY_ID_FIELD = UUID + ".entity.id";

    /** The Constant KUNDERA_ID_FIELD. */
    public static final String KUNDERA_ID_FIELD = UUID + ".kundera.id";

    /** The Constant ENTITY_INDEXNAME_FIELD. */
    public static final String ENTITY_INDEXNAME_FIELD = UUID + ".entity.indexname";

    /** The Constant ENTITY_CLASS_FIELD. */
    public static final String ENTITY_CLASS_FIELD = /* UUID + */"entity.class";

    /** The Constant PARENT_ID_FIELD. */
    public static final String PARENT_ID_FIELD = UUID + ".parent.id";

    /** The Constant PARENT_ID_CLASS. */
    public static final String PARENT_ID_CLASS = UUID + ".parent.class";

    
    public static final String LUCENE_INDEXER = "com.impetus.kundera.index.LuceneIndexer";
}
