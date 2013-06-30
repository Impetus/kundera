/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.query.Person;

/**
 * @author vivek.mishra junit for {@link ElementCollectionCacheManager}
 * 
 */
public class ElementCollectionCacheManagerTest
{

    @Test
    public void test()
    {
        Person p = new Person(); // create object.
        p.setAge(23);
        p.setPersonId("personId");

        ElementCollectionCacheManager manager = ElementCollectionCacheManager.getInstance();
        Assert.assertTrue(manager.isCacheEmpty());
        Assert.assertTrue(manager.getElementCollectionCache().isEmpty());
        Assert.assertNull(manager.getElementCollectionObjectName("personId", p));
        Assert.assertEquals(-1, manager.getLastElementCollectionObjectCount("personId"));

        manager.addElementCollectionCacheMapping("personId", p, "age#1");
        Assert.assertNotNull(manager.getElementCollectionObjectName("personId", p));
        Assert.assertEquals("age#1", manager.getElementCollectionObjectName("personId", p));
        Assert.assertNotNull(manager.getLastElementCollectionObjectCount("personId"));
        Assert.assertEquals(1, manager.getLastElementCollectionObjectCount("personId"));

        manager.addElementCollectionCacheMapping("personId", p, "personName#1");
        Assert.assertEquals("personName#1", manager.getElementCollectionObjectName("personId", p));

        try
        {
            manager.addElementCollectionCacheMapping("personId", p, "personName");
            manager.getLastElementCollectionObjectCount("personId");
            Assert.fail("Should have gone to catch block!");
        }
        catch (CacheException cex)
        {
            Assert.assertNotNull(cex.getMessage());
        }

        manager.clearCache();
        Assert.assertTrue(manager.isCacheEmpty());

    }

}
