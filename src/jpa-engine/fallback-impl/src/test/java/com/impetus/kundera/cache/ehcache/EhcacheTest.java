/**
 * Copyright 2013 Impetus Infotech.
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

package com.impetus.kundera.cache.ehcache;

import java.lang.reflect.Field;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.cache.Cache;
import com.impetus.kundera.entity.EhCacheEntity;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.Person.Day;

/**
 * Junit for ehcache.
 * @author vivek.mishra
 * 
 */
public class EhcacheTest
{

    private static final String PU = "ehacheTest";

    private EntityManagerFactory emf;

    private EntityManager em;

    protected Map propertyMap = null;

    @Before
    public void setUp() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        emf = Persistence.createEntityManagerFactory(PU, propertyMap);
        em = emf.createEntityManager();
    }

    @Test
    public void testDummy()
    {
        //do nothing.
    }
    // TODO:: enable it with #494.
//    @Test
    public void testEhCache()
    {
        EhCacheEntity entity1 = prepareData("1", 32);
        em.persist(entity1); // persist entity 1

        EhCacheEntity entity2 = prepareData("2", 32);
        em.persist(entity2); // persist entity 1

        Cache l2Cache = (Cache) em.getEntityManagerFactory().getCache();

        PersistenceDelegator persistenceDelegator = null;
        persistenceDelegator = getPersistenceDelegator(persistenceDelegator);

        // get node from first level cache.
        Node node1 = persistenceDelegator.getPersistenceCache().getMainCache().getNodeFromCache(entity1);

        // check if it is present in second level cache.
        EhCacheEntity foundNode1 = (EhCacheEntity) l2Cache.get(node1.getNodeId());
        
        Assert.assertNotNull(foundNode1);
        Assert.assertEquals(foundNode1,node1.getData()); // should be same object.

        // remove entity 1.
        em.remove(entity1);

        Node node2 = persistenceDelegator.getPersistenceCache().getMainCache().getNodeFromCache(entity2);

        Assert.assertNotNull(l2Cache.get(node2.getNodeId()));
        
        EhCacheEntity foundNode2 = (EhCacheEntity) l2Cache.get(node2.getNodeId());
        Assert.assertEquals(foundNode2,node2.getData()); // should be same object.
        Assert.assertNull(l2Cache.get(node1.getNodeId()));
        
        entity1.setAge(99);
        em.persist(entity1);
        em.flush();
        
        // get node from first level cache.
        node1 = persistenceDelegator.getPersistenceCache().getMainCache().getNodeFromCache(entity1);

        // check if it is present in second level cache.
        foundNode1 = (EhCacheEntity) l2Cache.get(node1.getNodeId());
        
        Assert.assertNotNull(foundNode1);
        Assert.assertEquals(foundNode1,node1.getData()); // should be same object.
        
        Assert.assertEquals(foundNode1.getAge(), new Integer(99));
        Assert.assertEquals(foundNode1.getAge(), entity1.getAge());

        
        em.clear(); // evict all.
        
        Assert.assertNull(l2Cache.get(node2.getNodeId()));
        
    }

    private PersistenceDelegator getPersistenceDelegator(PersistenceDelegator persistenceDelegator)
    {
        try
        {
            Field pd = em.getClass().getDeclaredField("persistenceDelegator");

            if (!pd.isAccessible())
            {
                pd.setAccessible(true);
            }

            persistenceDelegator = (PersistenceDelegator) pd.get(em);
            
        }
        catch (NoSuchFieldException e)
        {
            Assert.fail("Invalid configuration");
        }
        catch (SecurityException e)
        {
            Assert.fail("Invalid configuration");
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail("Invalid configuration");
        }
        catch (IllegalAccessException e)
        {
            Assert.fail("Invalid configuration");
        }
        return persistenceDelegator;
    }

    private EhCacheEntity prepareData(String rowKey, int age)
    {
        EhCacheEntity o = new EhCacheEntity();
        o.setPersonId(rowKey);
        o.setPersonName("vivek");
        o.setAge(age);
        o.setDay(Day.THURSDAY);
        return o;
    }

}
