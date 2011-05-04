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

/**
 * NonOperational Cache implementation.
 * 
 * @author animesh.kumar
 * 
 */
public class NonOperationalCache implements Cache {

	/* @see com.impetus.kundera.cache.Cache#size() */
	@Override
	public int size() {
		return 0;
	}

	/* @see com.impetus.kundera.cache.Cache#put(java.lang.Object, java.lang.Object) */
	@Override
	public void put(Object key, Object value) {
	}

	/* @see com.impetus.kundera.cache.Cache#remove(java.lang.Object) */
	@Override
	public boolean remove(Object key) {
		return true;
	}

	/* @see com.impetus.kundera.cache.Cache#clear() */
	@Override
	public void clear() {

	}

	/* @see com.impetus.kundera.cache.Cache#get(java.lang.Object) */
	@Override
	public Object get(Object key) {
		return null;
	}

}
