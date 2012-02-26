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
package com.impetus.kundera.cache;

import java.util.Map;

import javax.persistence.Cache;

/**
 * CacheProvider.
 * 
 * @author animesh.kumar
 */
public interface CacheProvider
{

    /**
     * Called once to load up the CacheManager.
     * 
     * @param properties
     *            the properties
     * @throws CacheException
     *             the cache exception
     */
    void init(Map<?, ?> properties);

    /**
     * Inits the.
     * 
     * @param cacheResourceName
     *            the cache resource name
     * @throws CacheException
     *             the cache exception
     */
    void init(String cacheResourceName);

    /**
     * Creates cache for a given name.
     * 
     * @param name
     *            the name
     * @return the cache
     * @throws CacheException
     *             the cache exception
     */
    Cache createCache(String name);

    /**
     * Returns cache for a given cache name.
     * 
     * @param name
     *            Cache Name
     * @return the cache
     * @throws CacheException
     *             the cache exception
     */
    Cache getCache(String name);

    /**
     * Shutdown cache.
     */
    void shutdown();

}
