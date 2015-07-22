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
package com.impetus.kundera.metadata.model;

import org.apache.commons.lang.StringUtils;

/**
 * The Class ClientMetadata.
 * 
 * @author amresh.singh
 */
public class ClientMetadata
{

    /** The client implementor. */
    private String clientImplementor;

    /** The index implementor. */
    private String indexImplementor;

    /** The autogen implementor. */
    private String autoGenImplementor;

    /**
     * @return the autoGenImplementor
     */
    public String getAutoGenImplementor()
    {
        return autoGenImplementor;
    }

    /**
     * @param autoGenImplementor the autoGenImplementor to set
     */
    public void setAutoGenImplementor(String autoGenImplementor)
    {
        this.autoGenImplementor = autoGenImplementor;
    }

    /** The Lucene index dir. */
    private String LuceneIndexDir;

    /**
     * Gets the client implementor.
     * 
     * @return the clientImplementor
     */
    public String getClientImplementor()
    {
        return clientImplementor;
    }

    /**
     * Sets the client implementor.
     * 
     * @param clientImplementor
     *            the clientImplementor to set
     */
    public void setClientImplementor(String clientImplementor)
    {
        this.clientImplementor = clientImplementor;
    }

    /**
     * Gets the index implementor.
     * 
     * @return the indexImplementor
     */
    public String getIndexImplementor()
    {
        return indexImplementor;
    }

    /**
     * Sets the index implementor.
     * 
     * @param indexImplementor
     *            the indexImplementor to set
     */
    public void setIndexImplementor(String indexImplementor)
    {
        this.indexImplementor = indexImplementor;
    }

    /**
     * Checks if is use secondry index.
     * 
     * @return the useSecondryIndex
     */
    public boolean isUseSecondryIndex()
    {
        // if lucene directory and indexer class both not present then return
        // true.

        return StringUtils.isEmpty(LuceneIndexDir) && StringUtils.isBlank(LuceneIndexDir)
                && StringUtils.isEmpty(indexImplementor) && StringUtils.isBlank(indexImplementor);
    }

    /**
     * Gets the lucene index dir.
     * 
     * @return the luceneIndexDir
     */
    public String getLuceneIndexDir()
    {
        return LuceneIndexDir;
    }

    /**
     * Sets the lucene index dir.
     * 
     * @param luceneIndexDir
     *            the luceneIndexDir to set
     */
    public void setLuceneIndexDir(String luceneIndexDir)
    {
        LuceneIndexDir = luceneIndexDir;
    }

}
