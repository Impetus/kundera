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
package com.impetus.client.hbase.crud;

import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.client.Client;

public class PersonHBaseTest extends BaseTest
{
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    private Map<Object, Object> col;

    private HBaseCli cli;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        cli = new HBaseCli();
        cli.startCluster();
        emf = Persistence.createEntityManagerFactory("hbaseTest");
        em = emf.createEntityManager();
        col = new java.util.HashMap<Object, Object>();
    }

    // @Test
    // public void testDummy()
    // {
    // // just to fix CI issue. TO BE DELETED!!!
    // }
    @Test
    public void onInsertHbase() throws Exception
    {
        init();
        PersonHBase personHBase = findById(PersonHBase.class, "1", em);
        Assert.assertNotNull(personHBase);
        Assert.assertEquals("vivek", personHBase.getPersonName());
        assertFindByName(em, "PersonHBase", PersonHBase.class, "vivek", "personName");
        assertFindByNameAndAge(em, "PersonHBase", PersonHBase.class, "vivek", "10", "personName");
        assertFindByNameAndAgeGTAndLT(em, "PersonHBase", PersonHBase.class, "vivek", "10", "20", "personName");
        assertFindByNameAndAgeBetween(em, "PersonHBase", PersonHBase.class, "vivek", "10", "15", "personName");
        assertFindByRange(em, "PersonHBase", PersonHBase.class, "1", "3", "personId");
        assertFindWithoutWhereClause(em, "PersonHBase", PersonHBase.class);
    }

    private void init()
    {
        if (!cli.isStarted)
        {
            cli.startCluster();
        }
        cli.createTable("PERSON");
        cli.addColumnFamily("PERSON", "PERSON_NAME");
        cli.addColumnFamily("PERSON", "AGE");
        // }
        Object p1 = prepareHbaseInstance("1", 10);
        Object p2 = prepareHbaseInstance("2", 20);
        Object p3 = prepareHbaseInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
    }

    // @Test
    public void onFilterOperation()
    {
        init();

        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get("hbaseTest");

        Filter f = new QualifierFilter();
        f = new SingleColumnValueFilter("PERSON_NAME".getBytes(), "PERSON_NAME".getBytes(), CompareOp.EQUAL,
                "vivek".getBytes());

        ((HBaseClient) client).setFilter(f);

        em.clear();
        // find by without where clause.
        Query q = em.createQuery("Select p from " + PersonHBase.class.getSimpleName() + " p");
        List<PersonHBase> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(3, results.size());
    }

    // @Test
    // public void onMergeHbase() {
    // em.persist(prepareHbaseInstance("1", 10));
    // PersonHBase personHBase = findById(PersonHBase.class, "1", em);
    // Assert.assertNotNull(personHBase);
    // Assert.assertEquals("vivek", personHBase.getPersonName());
    // personHBase.setPersonName("Newvivek");
    //
    // em.merge(personHBase);
    // assertOnMerge(em, "PersonHBase", PersonHBase.class);
    // o.add(PersonHBase.class);
    // }
    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {

        /*
         * * Delete is working, but as row keys are not deleted from cassandra,
         * so resulting in issue while reading back. // Delete
         * em.remove(em.find(Person.class, "1"));
         * em.remove(em.find(Person.class, "2"));
         * em.remove(em.find(Person.class, "3")); em.close(); emf.close(); em =
         * null; emf = null;
         */
        for (Object val : col.values())
        {
            em.remove(val);
        }
        em.close();
        emf.close();
        if (cli != null && cli.isStarted())
        {
            cli.stopCluster("PERSON");
        }
        LuceneCleanupUtilities.cleanLuceneDirectory("hbaseTest");
        // if (cli.isStarted)

    }
}
