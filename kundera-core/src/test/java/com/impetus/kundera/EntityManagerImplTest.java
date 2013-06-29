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

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.TemporalType;
import javax.persistence.TypedQuery;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.DummyDatabase;
import com.impetus.kundera.metadata.entities.SampleEntity;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.query.KunderaTypedQuery;

/**
 * @author vivek.mishra
 * 
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
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
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
        long t1 = System.currentTimeMillis();

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
            long t2 = System.currentTimeMillis();
            System.out.println("Time taken for 1 million dummy insert: " + (t2 - t1));
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
        //Persist
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
        //Persist
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
        
        //Modify record in dummy database directly
        SampleEntity se = (SampleEntity)DummyDatabase.INSTANCE.getSchema("KunderaTest").getTable("table").getRecord(new Integer(1));
        se.setCity("Singapore");     
        
        em.refresh(foundAfterMerge);        
        SampleEntity found2 = em.find(SampleEntity.class, 1);
        Assert.assertEquals("Singapore", found2.getCity());
        
        em.detach(foundAfterMerge);        
        em.clear();
        found = em.find(SampleEntity.class, 1);
        
        em.clear();        
        em.remove(found);
        SampleEntity foundAfterDeletion = em.find(SampleEntity.class, 1);
        Assert.assertNull(foundAfterDeletion);
    }

    
    @Test
    public void testNativeQuery()
    {
        final String nativeQuery = "Select * from persontable";
        Query query = em.createNativeQuery(nativeQuery, SampleEntity.class);
        
        Assert.assertNotNull(query);
        Assert.assertTrue(KunderaMetadata.INSTANCE.getApplicationMetadata().isNative(nativeQuery));
    }
    
    @Test
    public void testTypedQuery()
    {
        final String namedQuery = "Select s from SampleEntity s";
        
        TypedQuery<SampleEntity> query = em.createNamedQuery(namedQuery, SampleEntity.class);

        Assert.assertTrue(query.getClass().isAssignableFrom(KunderaTypedQuery.class));
        
        query.setMaxResults(100);
        Assert.assertEquals(100, query.getMaxResults());
        
        assertOnUnsupportedMethod(query);
        Assert.assertEquals(FlushModeType.AUTO, FlushModeType.AUTO);
        
        
    }

    /**
     * @param query
     */
    private void assertOnUnsupportedMethod(TypedQuery<SampleEntity> query)
    {
        try
        {
            query.setFlushMode(FlushModeType.AUTO);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setFlushMode is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.setFirstResult(1);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setFirstResult is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.getSingleResult();
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("getSingleResult is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.getFirstResult();
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("getFirstResult is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.setLockMode(LockModeType.NONE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setLockMode is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.getLockMode();
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("getLockMode is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.setParameter(0,new Date(),TemporalType.DATE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setParameter("param",new Date(),TemporalType.DATE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.setParameter(0,Calendar.getInstance(),TemporalType.DATE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }
        
        try
        {
            query.setParameter("param",Calendar.getInstance(),TemporalType.DATE);
        } catch(UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
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
