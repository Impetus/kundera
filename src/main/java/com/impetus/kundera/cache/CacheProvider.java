/*
 * Copyright 2010 Impetus Infotech.
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
 */
package com.impetus.kundera.cache;

import java.util.Map;

/**
 * CacheProvider.
 * 
 * @author animesh.kumar
 */
public interface CacheProvider {

    /**
	 * Called once to load up the CacheManager.
	 * 
	 * @param properties
	 *            the properties
	 * @throws CacheException
	 *             the cache exception
	 */
    void init(Map<?, ?> properties) throws CacheException;
    
    void init(String cacheResourceName) throws CacheException;

    /**
	 * Create cache for a given name.
	 * 
	 * @param name
	 *            the name
	 * @return the cache
	 * @throws CacheException
	 *             the cache exception
	 */
    Cache createCache(String name) throws CacheException;

    /**
	 * Shutdown cache.
	 */
    void shutdown();

}
