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
 * NonOperational cache provider.
 * 
 * @author animesh.kumar
 */
public class NonOperationalCacheProvider implements CacheProvider {
	
	/** The cache. */
	private Cache cache = new NonOperationalCache();

	/**
	 * Instantiates a new non operational cache provider.
	 */
	public NonOperationalCacheProvider () {
	}
	
	/* @see com.impetus.kundera.cache.CacheProvider#init(java.util.Map) */
	@Override
	public void init(Map<?, ?> properties) {
	}

	/* @see com.impetus.kundera.cache.CacheProvider#createCache(java.lang.String) */
	@Override
	public Cache createCache(String name) {
		return cache;
	}

	/* @see com.impetus.kundera.cache.CacheProvider#shutdown() */
	@Override
	public void shutdown() {
	}

	@Override
	public void init(String cacheResourceName) throws CacheException {
		// TODO Auto-generated method stub
		
	}

}
