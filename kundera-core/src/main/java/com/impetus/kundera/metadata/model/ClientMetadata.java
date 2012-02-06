/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
 * @author amresh.singh
 * 
 */
public class ClientMetadata
{
    private String clientImplementor;

    private String indexImplementor;

    private String LuceneIndexDir;

    /**
     * @return the clientImplementor
     */
    public String getClientImplementor()
    {
        return clientImplementor;
    }

    /**
     * @param clientImplementor
     *            the clientImplementor to set
     */
    public void setClientImplementor(String clientImplementor)
    {
        this.clientImplementor = clientImplementor;
    }

    /**
     * @return the indexImplementor
     */
    public String getIndexImplementor()
    {
        return indexImplementor;
    }

    /**
     * @param indexImplementor
     *            the indexImplementor to set
     */
    public void setIndexImplementor(String indexImplementor)
    {
        this.indexImplementor = indexImplementor;
    }

    /**
     * @return the useSecondryIndex
     */
    public boolean isUseSecondryIndex()
    {
        return StringUtils.isEmpty(LuceneIndexDir) && StringUtils.isBlank(LuceneIndexDir);
    }

    /**
     * @return the luceneIndexDir
     */
    public String getLuceneIndexDir()
    {
        return LuceneIndexDir;
    }

    /**
     * @param luceneIndexDir
     *            the luceneIndexDir to set
     */
    public void setLuceneIndexDir(String luceneIndexDir)
    {
        LuceneIndexDir = luceneIndexDir;
    }

}
