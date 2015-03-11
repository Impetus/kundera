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
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * @author Pragalbh Garg
 * 
 */
public class EmbeddablesCrudTest extends EmbeddablesBase
{

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PU);
        em = emf.createEntityManager();

    }

    @Test
    public void testCRUDOperations() throws Exception
    {
        init();
        testInsert();
        testMerge();
        testRemove();

    }

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
    }

    @AfterClass
    public static void tearDownAfterClass()
    {
        HBaseTestingUtils.dropSchema("HBaseNew");
        if (em != null)
        {
            em.close();
            em = null;
            emf.close();
            emf = null;
        }

    }

}
