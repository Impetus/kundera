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
package com.impetus.kundera.persistence;

import junit.framework.TestCase;

import com.impetus.kundera.cache.Cache;
import com.impetus.kundera.cache.CacheProvider;
import com.impetus.kundera.entity.Person;

/**
 * @author amresh.singh
 * 
 */
public class EntityManagerSessionTest extends TestCase
{
    EntityManagerSession ems;

    CacheProvider cacheProvider;

    Cache cache;

    String cacheResource = "/ehcache-test.xml";;

    String cacheProviderClassName = "com.impetus.kundera.cache.ehcache.EhCacheProvider";

    Person person1;

    Person person2;

    protected void setUp() throws Exception
    {
        super.setUp();

        Class<CacheProvider> cacheProviderClass = (Class<CacheProvider>) Class.forName(cacheProviderClassName);
        cacheProvider = cacheProviderClass.newInstance();
        cacheProvider.init(cacheResource);

        cache = (Cache) cacheProvider.createCache("Kundera");
        ems = new EntityManagerSession(cache);

        person1 = new Person("1", "Amresh", "Singh");
        person2 = new Person("2", "Vivek", "Mishra");
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
        ems.clear();
    }

    public void test()
    {
        assertNotNull(ems);

        // Store objects into session
        ems.store(person1.getPersonId(), person1);
        assertEquals(1, ems.getL2Cache().size());
        ems.store(person2.getPersonId(), person2);
        assertEquals(2, ems.getL2Cache().size());

        // Lookup object from session
        Person p1 = ems.lookup(Person.class, person1.getPersonId());
        assertNotNull(p1);
        assertEquals(person1.getPersonId(), p1.getPersonId());
        assertEquals(person1.getFirstName(), p1.getFirstName());
        assertEquals(person1.getLastName(), p1.getLastName());

        // Remove object from session
        ems.remove(Person.class, person1.getPersonId());
        assertNotNull(ems);
        assertEquals(1, ems.getL2Cache().size());

        // Clear session
        ems.clear();
        assertNotNull(ems);
        assertEquals(0, ems.getL2Cache().size());

    }

}
