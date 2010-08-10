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
 * Implementors define a caching algorithm. All implementors
 * <b>must</b> be threadsafe.
 * 
 * @author animesh.kumar
 */
public interface Cache {

	int size();

    /**
     * Get an item from the cache.
     * 
     * @param o
     * @return
     */
    Object get (Object key);

    /**
     * Add an item to the cache
     * 
     * @param o
     * @param o1
     */
    void put(Object key, Object value);

    /**
     * Removes an item from cache
     * 
     * @param o
     * @return
     */
    boolean remove(Object key);

    /**
     * Clears cache 
     */
    void clear();
}
