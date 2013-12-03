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
package com.impetus.kundera.cache;

/**
 * NonOperational Cache implementation.
 * 
 * @author animesh.kumar
 * 
 */
public class NonOperationalCache implements Cache, javax.persistence.Cache
{

    /* @see com.impetus.kundera.cache.Cache#size() */
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.cache.Cache#size()
     */
    @Override
    public int size()
    {
        return 0;
    }

    /*
     * @see com.impetus.kundera.cache.Cache#put(java.lang.Object,
     * java.lang.Object)
     */
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.cache.Cache#put(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void put(final Object key, final Object value)
    {
    }

    /* @see com.impetus.kundera.cache.Cache#get(java.lang.Object) */
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.cache.Cache#get(java.lang.Object)
     */
    @Override
    public Object get(final Object key)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Cache#contains(java.lang.Class, java.lang.Object)
     */
    @Override
    public boolean contains(Class paramClass, Object paramObject)
    {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Cache#evict(java.lang.Class, java.lang.Object)
     */
    @Override
    public void evict(Class paramClass, Object paramObject)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // do nothing.
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Cache#evict(java.lang.Class)
     */
    @Override
    public void evict(Class paramClass)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // do nothing.
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Cache#evictAll()
     */
    @Override
    public void evictAll()
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // do nothing.

    }

    @Override
    public <T> T unwrap(Class<T> arg0)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // do nothing.
        return null;
    }

}