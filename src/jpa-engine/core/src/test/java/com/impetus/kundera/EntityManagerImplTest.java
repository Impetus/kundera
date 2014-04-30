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
package com.impetus.kundera;

import java.util.HashMap;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.LockModeType;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.commons.lang.NotImplementedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.DummyDatabase;
import com.impetus.kundera.metadata.entities.SampleEntity;
import com.impetus.kundera.persistence.EntityManagerImpl;
import com.impetus.kundera.polyglot.entities.PersonBMM;
import com.impetus.kundera.query.Person;

/**
 * @author vivek.mishra junit for {@link EntityManagerImpl}
 */
public class EntityManagerImplTest
{

    private EntityManager em;

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(EntityManagerImplTest.class);

    private EntityManagerFactory emf;

    @Before
    public void setUp()
    {

        emf = Persistence.createEntityManagerFactory("kunderatest");

        em = emf.createEntityManager();
    }

    /**
     * On test persist.
     * 
     */
    @Test
    public void testPersist()
    {
        try
        {
            for (int i = 1; i <= 1000000; i++)
            {
                final SampleEntity entity = new SampleEntity();
                entity.setKey(i);
                entity.setName("name" + i);
                if (i % 5000 == 0)
                {
                    em.clear();
                }

                em.persist(entity);

            }
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }
    }

    @After
    public void tearDown()
    {
        if (em != null)
            em.close();
        if (emf != null)
            emf.close();

        dropDatabase();
    }

    private void dropDatabase()
    {
        DummyDatabase.INSTANCE.dropDatabase();
    }

    @Test
    public void testSingleEntityCRUD_EmNotCleared()
    {
        // Persist
        final SampleEntity entity = new SampleEntity();
        entity.setKey(1);
        entity.setName("Amry");
        entity.setCity("Delhi");
        em.persist(entity);
        SampleEntity found = em.find(SampleEntity.class, 1);
        assertSampleEntity(found);

        Assert.assertTrue(em.contains(found));

        found.setName("Xamry");
        found.setCity("Noida");
        em.merge(found);

        SampleEntity foundAfterMerge = em.find(SampleEntity.class, 1);
        assertUpdatedSampleEntity(foundAfterMerge);
        em.flush();

        em.remove(foundAfterMerge);
        SampleEntity foundAfterDeletion = em.find(SampleEntity.class, 1);
        Assert.assertNull(foundAfterDeletion);
    }

    @Test
    public void testSingleEntityCRUD_EmCleared()
    {
        // Persist
        final SampleEntity entity = new SampleEntity();
        entity.setKey(1);
        entity.setName("Amry");
        entity.setCity("Delhi");
        em.persist(entity);

        Assert.assertTrue(em.contains(entity));
        em.clear();
        Assert.assertFalse(em.contains(entity));

        SampleEntity found = em.find(SampleEntity.class, 1, new HashMap<String, Object>());

        assertSampleEntity(found);

        found.setName("Xamry");
        found.setCity("Noida");
        em.clear();
        em.merge(found);

        SampleEntity foundAfterMerge = em.find(SampleEntity.class, 1);
        assertUpdatedSampleEntity(foundAfterMerge);

        // Modify record in dummy database directly
        SampleEntity se = (SampleEntity) DummyDatabase.INSTANCE.getSchema("KunderaTest").getTable("table")
                .getRecord(new Integer(1));
        se.setCity("Singapore");

        em.refresh(foundAfterMerge);
        SampleEntity found2 = em.find(SampleEntity.class, 1);
        Assert.assertEquals("Singapore", found2.getCity());

        em.detach(foundAfterMerge);
        em.clear();
        found = em.find(SampleEntity.class, 1);

        em.remove(found);
        em.clear();
        SampleEntity foundAfterDeletion = em.find(SampleEntity.class, 1);
        Assert.assertNull(foundAfterDeletion);
    }

    @Test
    public void testNativeQuery()
    {
        final String nativeQuery = "Select * from persontable";
        Query query = em.createNativeQuery(nativeQuery, SampleEntity.class);

        Assert.assertNotNull(query);
        // Assert.assertTrue(kunderaMetadata.getApplicationMetadata().isNative(nativeQuery));
    }

    @Test
    public void testUnsupportedMethod()
    {

        try
        {
            // find(Class<T> paramClass, Object paramObject, LockModeType
            // paramLockModeType)
            em.find(PersonBMM.class, null, LockModeType.NONE);
            Assert.fail("Should have gone to catch block!");
        }
        catch (NotImplementedException niex)
        {
            Assert.assertNotNull(niex);
        }

        try
        {
            // find(Class<T> arg0, Object arg1, LockModeType arg2, Map<String,
            // Object> arg3)
            em.find(PersonBMM.class, null, LockModeType.NONE, null);
            Assert.fail("Should have gone to catch block!");
        }
        catch (NotImplementedException niex)
        {
            Assert.assertNotNull(niex);
        }

        try
        {
            // createNativeQuery(String sqlString)
            em.createNativeQuery("Query without class is not supported");
        }
        catch (NotImplementedException niex)
        {
            Assert.fail();
        }
        try
        {
            // createNativeQuery(String sqlString, String resultSetMapping)
            em.createNativeQuery("Query without class is not supported", "noreuslt");
            Assert.fail("Should have gone to catch block!");
        }
        catch (NotImplementedException niex)
        {
            Assert.assertNotNull(niex);
        }
        try
        {
            // getReference(Class<T> entityClass, Object primaryKey)
            em.getReference(Person.class, null);
            Assert.fail("Should have gone to catch block!");
        }
        catch (NotImplementedException niex)
        {
            Assert.assertNotNull(niex);
        }
        try
        {
            // lock(Object paramObject, LockModeType paramLockModeType,
            // Map<String, Object> paramMap)
            em.lock(null, LockModeType.NONE, null);
            Assert.fail("Should have gone to catch block!");
        }
        catch (NotImplementedException niex)
        {
            Assert.assertNotNull(niex);
        }
        try
        {
            // refresh(Object paramObject, LockModeType paramLockModeType)
            em.refresh(null, LockModeType.NONE);
            Assert.fail("Should have gone to catch block!");
        }
        catch (NotImplementedException niex)
        {
            Assert.assertNotNull(niex);
        }
        try
        {
            // refresh(Object paramObject, LockModeType paramLockModeType,
            // Map<String, Object> paramMap)
            em.refresh(null, LockModeType.NONE, null);
            Assert.fail("Should have gone to catch block!");
        }
        catch (NotImplementedException niex)
        {
            Assert.assertNotNull(niex);
        }
        try
        {
            // getLockMode(Object paramObject)
            em.getLockMode(null);
            Assert.fail("Should have gone to catch block!");
        }
        catch (NotImplementedException niex)
        {
            Assert.assertNotNull(niex);
        }
        try
        {
            // unwrap(Class<T> paramClass)
            em.unwrap(null);
            Assert.fail("Should have gone to catch block!");
        }
        catch (NotImplementedException niex)
        {
            Assert.assertNotNull(niex);
        }

    }

    /**
     * @param found
     */
    private void assertSampleEntity(SampleEntity found)
    {
        Assert.assertNotNull(found);
        Assert.assertEquals(new Integer(1), found.getKey());
        Assert.assertEquals("Amry", found.getName());
        Assert.assertEquals("Delhi", found.getCity());
    }

    /**
     * @param found
     */
    private void assertUpdatedSampleEntity(SampleEntity found)
    {
        Assert.assertNotNull(found);
        Assert.assertEquals(new Integer(1), found.getKey());
        Assert.assertEquals("Xamry", found.getName());
        Assert.assertEquals("Noida", found.getCity());
    }
}
