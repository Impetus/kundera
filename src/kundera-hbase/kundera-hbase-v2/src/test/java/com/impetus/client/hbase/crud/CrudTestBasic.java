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
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.crud.PersonHBase.Day;
import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * @author Pragalbh Garg
 * 
 */
public class CrudTestBasic
{
    /** The emf. */
    protected static EntityManagerFactory emf;

    /** The em. */
    protected static EntityManager em;

    protected HBaseCli cli;

    @Before
    public void setUp() throws Exception
    {
        cli = new HBaseCli();
        cli.startCluster();
        emf = Persistence.createEntityManagerFactory("crudTest");
        em = emf.createEntityManager();
    }

    @Test
    public void testCRUDOperations() throws Exception
    {
        testInsert();
        testMerge();
        testRemove();

    }

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
        em.persist(p);
        PersonHBase p2 = em.find(PersonHBase.class, "1");
        Assert.assertNotNull(p2);
        Assert.assertEquals("1", p2.getPersonId());
        Assert.assertEquals("pragalbh", p2.getPersonName());

    }

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

    private void testRemove()
    {
        em.clear();
        PersonHBase p = em.find(PersonHBase.class, "1");
        em.remove(p);
        em.clear();
        PersonHBase p1 = em.find(PersonHBase.class, "1");
        Assert.assertNull(p1);
    }

    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        HBaseTestingUtils.dropSchema("HBaseNew");
        HBaseCli.stopCluster();
    }

}
