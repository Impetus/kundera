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
package com.impetus.kundera.cache.ehcache;

import javax.persistence.Cache;

import junit.framework.TestCase;

import com.impetus.kundera.cache.CacheException;
import com.impetus.kundera.entity.PersonnelDTO;

/**
 * The Class EhCacheProviderTest.
 * 
 * @author amresh.singh
 */
public class EhCacheProviderTest extends TestCase
{

    /** The cache provider. */
    EhCacheProvider cacheProvider;

    /** The cache resource. */
    String cacheResource = "/ehcache-test.xml";;

    /** The cache name. */
    String cacheName = "Kundera";

    /** The person1. */
    PersonnelDTO person1;

    /** The person2. */
    PersonnelDTO person2;

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception
    {
        super.setUp();

        cacheProvider = new EhCacheProvider();

        person1 = new PersonnelDTO("1", "Amresh", "Singh");
        person2 = new PersonnelDTO("2", "Vivek", "Mishra");

    }

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception
    {
        super.tearDown();
        cacheProvider.shutdown();
    }

    /**
     * Test method for.
     * 
     * {@link com.impetus.kundera.cache.ehcache.EhCacheProvider#createCache(java.lang.String)}
     * .
     */
    public void testCreateCache()
    {
        // Initialize Cache Provider
        assertNotNull(cacheProvider);
        try
        {
            cacheProvider.init(cacheResource);
        }
        catch (CacheException e)
        {
            fail(e.getMessage());
        }

        assertNotNull(cacheProvider.getCacheManager());

        // Initialize Cache
        Cache cache = null;
        try
        {
            cache = cacheProvider.createCache(cacheName);
        }
        catch (CacheException e)
        {
            fail(e.getMessage());
        }
        assertNotNull(cache);
        assertEquals(cache.getClass(), EhCacheWrapper.class);

        EhCacheWrapper ehCache = (EhCacheWrapper) cache;

        assertEquals(0, ehCache.size());

        // Store objects into cache
        ehCache.put(person1.getClass() + "_" + person1.getPersonId(), person1);
        assertEquals(1, ehCache.size());
        ehCache.put(person2.getClass() + "_" + person2.getPersonId(), person2);
        assertEquals(2, ehCache.size());

        // Lookup objects from cache
        Object o = ehCache.get(person1.getClass() + "_" + person1.getPersonId());
        assertEquals(PersonnelDTO.class, o.getClass());
        PersonnelDTO p1 = (PersonnelDTO) o;
        assertNotNull(p1);
        assertEquals("1", p1.getPersonId());
        assertEquals("Amresh", p1.getFirstName());
        assertEquals("Singh", p1.getLastName());

        // Remove object from cache
        ehCache.evict(PersonnelDTO.class, PersonnelDTO.class + "_" + person1.getPersonId());
        assertEquals(1, ehCache.size());

        // Clear cache
        cacheProvider.clearAll();
        assertEquals(0, ehCache.size());
    }
}
