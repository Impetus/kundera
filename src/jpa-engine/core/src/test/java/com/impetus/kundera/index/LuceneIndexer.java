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
 * Provides indexing functionality using lucene library.
 * 
 * @author amresh.singh
 */
public class LuceneIndexer
{

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(LuceneIndexer.class);


    /** The indexer. */
    private static LuceneIndexer indexer;

    /**
     * Gets the single instance of LuceneIndexer.
     * 
     * @param analyzer
     *            the analyzer
     * @param lucDirPath
     *            the luc dir path
     * @return single instance of LuceneIndexer
     */
    public static synchronized LuceneIndexer getInstance(String lucDirPath)
    {
        return indexer;
    }

}
