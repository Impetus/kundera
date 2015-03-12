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
/*
 * author: karthikp.manchala
 */
package com.impetus.client.hbase.crud.embedded;

import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * The Class EmbeddablesCrudTest.
 * 
 * @author Pragalbh Garg
 */
public class EmbeddablesCrudTest extends EmbeddablesBase
{

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
        init();
        testInsert();
        testMerge();
        testRemove();

    }

    /**
     * Test remove.
     */
    private void testRemove()
    {
        PersonEmbed person1 = em.find(PersonEmbed.class, 1);
        PersonEmbed person2 = em.find(PersonEmbed.class, 2);
        em.remove(person1);
        em.remove(person2);
        em.clear();

        person1 = em.find(PersonEmbed.class, 1);
        person2 = em.find(PersonEmbed.class, 2);
        Assert.assertNull(person1);
        Assert.assertNull(person2);
    }

    /**
     * Test merge.
     */
    private void testMerge()
    {
        PersonEmbed person1 = em.find(PersonEmbed.class, 1);
        person1.setEmail("pg@impetus.com");
        person1.getProfessionalDetails().setCompany("impetus ilabs");
        em.merge(person1);
        em.clear();
        person1 = em.find(PersonEmbed.class, 1);
        p1.setEmail("pg@impetus.com");
        p1.getProfessionalDetails().setCompany("impetus ilabs");
        assertPerson(p1, person1);
    }

    /**
     * Test insert.
     */
    private void testInsert()
    {
        em.persist(p1);
        em.persist(p2);
        em.clear();
        PersonEmbed person1 = em.find(PersonEmbed.class, 1);
        PersonEmbed person2 = em.find(PersonEmbed.class, 2);
        assertPerson(p1, person1);
        assertPerson(p2, person2);

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
     */
    @AfterClass
    public static void tearDownAfterClass()
    {
        emf.close();
        emf = null;
        HBaseTestingUtils.dropSchema(SCHEMA);
    }

}