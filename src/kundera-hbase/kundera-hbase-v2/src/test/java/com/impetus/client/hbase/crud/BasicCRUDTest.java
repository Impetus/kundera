/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.crud;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.crud.PersonHBase.Day;
import com.impetus.client.hbase.crud.PersonHBase.Month;
import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * The Class CrudTestBasic.
 * 
 * @author Pragalbh Garg
 */
public class BasicCRUDTest
{
    /** The Constant SCHEMA. */
    private static final String SCHEMA = "HBaseNew";

    /** The Constant HBASE_PU. */
    private static final String HBASE_PU = "crudTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(HBASE_PU);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    /**
     * Test crud operations.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void testCRUDOperations() throws Exception
    {
        testInsert();
        testMerge();
        testRemove();
    }

    /**
     * Test insert.
     * 
     * @throws Exception
     *             the exception
     */
    private void testInsert() throws Exception
    {
        em.clear();
        PersonHBase p = new PersonHBase();
        p.setPersonId("1");
        p.setPersonName("pragalbh");
        p.setAge(22);
        p.setMonth(Month.JAN);
        p.setDay(Day.FRIDAY);
        em.persist(p);
        em.clear();
        PersonHBase p2 = em.find(PersonHBase.class, "1");
        Assert.assertNotNull(p2);
        Assert.assertEquals("1", p2.getPersonId());
        Assert.assertEquals("pragalbh", p2.getPersonName());

    }

    /**
     * Test merge.
     */
    private void testMerge()
    {
        em.clear();
        PersonHBase p = em.find(PersonHBase.class, "1");
        p.setPersonName("devender");
        em.merge(p);
        em.clear();
        PersonHBase p1 = em.find(PersonHBase.class, "1");
        Assert.assertEquals("devender", p1.getPersonName());
    }

    /**
     * Test remove.
     */
    private void testRemove()
    {
        em.clear();
        PersonHBase p = em.find(PersonHBase.class, "1");
        em.remove(p);
        em.clear();
        PersonHBase p1 = em.find(PersonHBase.class, "1");
        Assert.assertNull(p1);
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        emf.close();
        emf = null;
        HBaseTestingUtils.dropSchema(SCHEMA);
    }

}
