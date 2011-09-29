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
package com.impetus.kundera.cache.ehcache;

import net.sf.ehcache.Element;

import org.apache.commons.lang.NotImplementedException;

import com.impetus.kundera.cache.Cache;

/**
 * Cache implementation using Ehcache.
 * 
 * @author animesh.kumar
 */
public class EhCacheWrapper implements Cache
{

    /** The ehcache. */
    private net.sf.ehcache.Cache ehcache;

    /**
     * Instantiates a new eh cache wrapper.
     * 
     * @param ehcache
     *            the ehcache
     */
    public EhCacheWrapper(net.sf.ehcache.Cache ehcache)
    {
        this.ehcache = ehcache;
    }

    /* @see com.impetus.kundera.cache.Cache#get(java.lang.Object) */
    @Override
    public Object get(Object key)
    {
        Element element = ehcache.get(key);
        return element == null ? null : element.getObjectValue();
    }

    /*
     * @see com.impetus.kundera.cache.Cache#put(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void put(Object key, Object value)
    {
        ehcache.put(new Element(key, value));
    }

    /* @see com.impetus.kundera.cache.Cache#size() */
    @Override
    public int size()
    {
        return ehcache.getSize();
    }

    @Override
    public boolean contains(Class arg0, Object arg1)
    {
        return (ehcache.get(arg1) != null);
    }

    @Override
    public void evict(Class arg0)
    {
        // TODO Can we use Class with ehcache
        throw new NotImplementedException("TODO");
    }

    @Override
    public void evict(Class arg0, Object arg1)
    {
        // TODO Can we use Class with ehcache
        ehcache.remove(arg1);
    }

    @Override
    public void evictAll()
    {
        ehcache.removeAll();
    }

}
