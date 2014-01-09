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

/**
 * @author amresh.singh
 * 
 */
public class LuceneIndexingException extends IndexingException
{
    /**
     * 
     */
    private static final long serialVersionUID = -9021996457355536038L;

    /**
     * 
     */
    public LuceneIndexingException()
    {
    }

    /**
     * @param arg0
     * @param arg1
     */
    public LuceneIndexingException(String arg0, Throwable arg1)
    {
        super(arg0, arg1);

    }

    /**
     * @param arg0
     */
    public LuceneIndexingException(String arg0)
    {
        super(arg0);

    }

    /**
     * @param arg0
     */
    public LuceneIndexingException(Throwable arg0)
    {
        super(arg0);

    }

}
