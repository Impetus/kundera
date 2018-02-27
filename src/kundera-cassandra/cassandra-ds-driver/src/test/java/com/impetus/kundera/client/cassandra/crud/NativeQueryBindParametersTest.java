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
package com.impetus.kundera.client.cassandra.crud;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.BaseTest;
import com.impetus.client.crud.Month;
import com.impetus.client.crud.PersonCassandra;
import com.impetus.client.crud.PersonCassandra.Day;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * Test case to perform native queries with bind parameters
 * 
 * @author dev@antonyudin.com
 */
public class NativeQueryBindParametersTest extends BaseTest {
    private static final String _PU = "cassandra_ds_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager entityManager;

    protected Map propertyMap = null;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception {

        CassandraCli.cassandraSetUp();
        System.setProperty("cassandra.start_native_transport", "true");

        if (propertyMap == null) {
            propertyMap = new HashMap();
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        }

        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);
        entityManager = emf.createEntityManager();

        final List<PersonCassandra> entities = new ArrayList<>();

        for (int i = 0; i < 10; i++) {

            final PersonCassandra entity = new PersonCassandra();
            entity.setPersonId(Integer.toString(i));
            entity.setPersonName("name" + i);
            entity.setAge(10 + i);
            entity.setDay(Day.thursday);
            entity.setMonth(Month.APRIL);

            entities.add(entity);
        }

        for (PersonCassandra entity : entities) {
            entityManager.persist(entity);
        }

        entityManager.flush();
        entityManager.clear();

    }

    /**
     * test native queries with named bind parameters
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onTestNativeQueriesNamedParametersInteger() throws Exception {

        final List<Object[]> result = entityManager
            .createNativeQuery("SELECT " + "\"personId\", \"PERSON_NAME\", \"AGE\" " + "FROM " + "\"PERSON\" "
                + "WHERE " + "\"AGE\" = :age " + "ALLOW FILTERING")
            .setParameter("age", 10 + 3).getResultList();

        Assert.assertNotNull(result);
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0)[0], "3");
        Assert.assertEquals(result.get(0)[1], "name" + 3);
        Assert.assertEquals(result.get(0)[2], 10 + 3);

        entityManager.clear();
    }

    /**
     * test native queries with named bind parameters
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onTestNativeQueriesNamedParametersString() throws Exception {

        final List<Object[]> result =
            entityManager.createNativeQuery("SELECT " + "\"personId\", \"PERSON_NAME\" " + "FROM " + "\"PERSON\" "
                + "WHERE " + "\"personId\" = :personId").setParameter("personId", "3").getResultList();

        Assert.assertNotNull(result);
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0)[0], "3");
        Assert.assertEquals(result.get(0)[1], "name" + 3);

        entityManager.clear();
    }

    /**
     * test native queries with indexed bind parameters
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onTestNativeQueriesIndexedParametersString() throws Exception {

        final List<Object[]> result = entityManager
            .createNativeQuery(
                "SELECT " + "\"personId\", \"PERSON_NAME\" " + "FROM " + "\"PERSON\" " + "WHERE " + "\"personId\" = ?")
            .setParameter(1, "2").getResultList();

        Assert.assertNotNull(result);
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0)[0], "2");
        Assert.assertEquals(result.get(0)[1], "name" + 2);

        entityManager.clear();
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception {
        entityManager.close();
        emf.close();
        CassandraCli.dropKeySpace("KunderaExamples");
    }

}
