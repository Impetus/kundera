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
package com.impetus.kundera.metadata.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.event.PersonEventDispatch;

/**
 * @author Kuldeep Mishra
 * 
 */
public class EntityMetadataTest
{
    private String persistenceUnit = "metaDataTest";

    private EntityManagerFactoryImpl emf;
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = getEntityManagerFactory(null);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {

    }

    /**
     * Gets the entity manager factory.
     * 
     * @param useLucene
     * @param property
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory(String property)
    {
        return (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory(persistenceUnit);
    }

    @Test
    public void testCallbackMethodsForLucene()
    {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("index.home.dir", "lucene");

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("kunderatest", props);
        EntityManager em = emf.createEntityManager();

        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance(), PersonEventDispatch.class);
        Assert.assertNotNull(m.toString());

        PersonEventDispatch person = new PersonEventDispatch();
        person.setFirstName("vivek");
        person.setLastName("mishra");
        person.setPersonId("1_p");

        em.persist(person);

        em.clear();

        PersonEventDispatch result = em.find(PersonEventDispatch.class, "1_p");
        Assert.assertEquals(result.getLastName(), "Post Load");

        onFindCallBack(em);
        em.refresh(result);

        Assert.assertEquals(result.getLastName(), "Post Load");

        em.close();
        emf.close();
    }

    private void onFindCallBack(EntityManager em)
    {
        String query = "Select p from PersonEventDispatch p";

        Query q = em.createQuery(query);

        List<PersonEventDispatch> results = q.getResultList();

        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void testColumn()
    {
        try
        {
            Column column = new Column("EMP_NAME", Employe.class.getDeclaredField("empName"));
            column.setIndexable(true);
            Assert.assertTrue(column.isIndexable());
            Assert.assertEquals("empName", column.getField().getName());
            Assert.assertEquals("EMP_NAME", column.getName());

            column = new Column("AGE", Employe.class.getDeclaredField("age"), true);
            Assert.assertTrue(column.isIndexable());
            Assert.assertEquals("age", column.getField().getName());
            Assert.assertEquals("AGE", column.getName());
        }
        catch (SecurityException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (NoSuchFieldException e)
        {
            Assert.fail(e.getMessage());
        }
    }
}
