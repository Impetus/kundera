/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */package com.impetus.kundera.query;

import java.util.Map;

import com.impetus.kundera.index.Indexer;

/**
 * @author vivek.mishra
 * 
 * Implementation of {@link Indexer} interface.
 *
 */
public class CoreIndexer implements Indexer
{

    
    /* (non-Javadoc)
     * @see com.impetus.kundera.index.Indexer#index(java.lang.Class, java.util.Map)
     */
    @Override
    public void index(Class entityClazz, Map<String, Object> values, Object parentId, Class clazz)
    {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.index.Indexer#search(java.lang.String, int, int)
     */
    @Override
    public Map<String, Object> search(Class<?> clazz, String queryString, int start, int count)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.index.Indexer#search(java.lang.Class, java.lang.Class, java.lang.Object, int, int)
     */
    @Override
    public Map<String, Object> search(String query, Class<?> parentClass, Class<?> childClass, Object entityId, int start, int count)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.index.Indexer#unIndex(java.lang.Class, java.lang.Object)
     */
    @Override
    public void unIndex(Class entityClazz, Object entity)
    {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.index.Indexer#close()
     */
    @Override
    public void close()
    {
        // TODO Auto-generated method stub

    }

}
