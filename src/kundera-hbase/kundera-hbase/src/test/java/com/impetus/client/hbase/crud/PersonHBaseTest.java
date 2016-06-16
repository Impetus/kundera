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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.client.hbase.crud.PersonHBase.Day;
import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

import junit.framework.Assert;

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

    @Test
    public void onInsertHbase() throws Exception
    {
        Query findQuery = em.createQuery("Select p from PersonHBase p");
        List<PersonHBase> allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        findQuery = em.createQuery("Select p from PersonHBase p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        findQuery = em.createQuery("Select p.age from PersonHBase p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());
        init();
        em.clear();
        PersonHBase personHBase = findById(PersonHBase.class, "1", em);

        Assert.assertNotNull(personHBase);
        Assert.assertEquals("vivek", personHBase.getPersonName());
        Assert.assertEquals(Day.MONDAY, personHBase.getDay());
        Assert.assertEquals(Month.MARCH, personHBase.getMonth());

        assertFindByName(em, "PersonHBase", PersonHBase.class, "vivek", "personName");
        assertFindByNameAndAge(em, "PersonHBase", PersonHBase.class, "vivek", "10", "personName");
        assertFindByNameAndAgeGTAndLT(em, "PersonHBase", PersonHBase.class, "vivek", "10", "20", "personName");
        assertFindByNameAndAgeBetween(em, "PersonHBase", PersonHBase.class, "vivek", "10", "15", "personName");
        assertFindByRange(em, "PersonHBase", PersonHBase.class, "1", "3", "personId");
        assertFindWithoutWhereClause(em, "PersonHBase", PersonHBase.class);
        assertFindByRowIds(em, "PersonHBase", PersonHBase.class, "1,2,6", "personId", 2);
        assertFindByRowIds(em, "PersonHBase", PersonHBase.class, "1,2,3", "personId", 3);
        selectIdQuery();
        personHBase.setPersonName("Bob");
        em.merge(personHBase);

        personHBase = findById(PersonHBase.class, "2", em);
        personHBase.setPersonName("John");
        em.merge(personHBase);
        // test case for In clause query
        assertFindByFieldValues(em, "PersonHBase", PersonHBase.class, "Bob,vivek", "personName", 2);
        assertFindByFieldValues(em, "PersonHBase", PersonHBase.class, "John,vivek,Bob", "personName", 3);
        assertFindByFieldValues(em, "PersonHBase", PersonHBase.class, "John,xyz", "personName", 1);

    }

    private void selectIdQuery()
    {
        String query = "select p.personId from PersonHBase p";
        Query q = em.createQuery(query);
        List<PersonHBase> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());

        query = "select p from PersonHBase p";
        com.impetus.kundera.query.Query queryObject = (com.impetus.kundera.query.Query) em.createQuery(query);
        queryObject.setFetchSize(1);

        Iterator<PersonHBase> resultIterator = queryObject.iterate();
        PersonHBase person = null;
        int counter = 0;
        while (resultIterator.hasNext())
        {
            counter++;
            person = resultIterator.next();
            Assert.assertNotNull(person.getPersonId());
            Assert.assertNotNull(person.getPersonName());
        }

        Assert.assertEquals(1, counter);

        query = "Select p.personId from PersonHBase p where p.personName = vivek";
        // // find by name.
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(3, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());

        q = em.createQuery("Select p.personId from PersonHBase p where p.personName = vivek and p.age > " + 10);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(2, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());

        q = em.createQuery("Select p.personId from PersonHBase p where p.personName = john OR p.age > " + 15);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(1, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());

        q = em.createQuery("Select p.personId from PersonHBase p where p.personName = xyz OR p.age = " + 15);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(1, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());

        q = em.createQuery("Select p.personId from PersonHBase p where p.personName = xyz OR p.age = " + 45);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertTrue(results.isEmpty());
    }

    //TODO: uncomment after upgrading hbase version
    //@Test
    public void onInsertLuceneHbase() throws Exception
    {
        // enabled for es indexing.
        Builder builder = Settings.settingsBuilder();
        builder.put("path.home", "target/data");
        Node node = new NodeBuilder().settings(builder).node();
        
        Map<String, Object> puProperties = new HashMap<String, Object>();
        puProperties.put("kundera.indexer.class", "com.impetus.client.es.index.ESIndexer");
        // puProperties.put("index.home.dir", "./lucene"); // uncomment for
        // lucene

        EntityManagerFactory emfLucene = Persistence.createEntityManagerFactory("hbaseTest", puProperties);
        EntityManager emLucene = emfLucene.createEntityManager();

        Object p1 = prepareHbaseInstance("1", 10);
        Object p2 = prepareHbaseInstance("2", 20);
        Object p3 = prepareHbaseInstance("3", 15);
        emLucene.persist(p1);
        emLucene.persist(p2);
        emLucene.persist(p3);

        Thread.sleep(1000);

        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
        emLucene.flush();
        emLucene.clear();

        Query q = emLucene.createQuery("Select p from PersonHBase p where p.personName = vivek1 OR p.age= 10");

        List<PersonHBase> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(1, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertEquals("vivek", results.get(0).getPersonName());
        Assert.assertEquals(10, results.get(0).getAge().intValue());

        q = emLucene
                .createQuery("Select p.personId, p.personName from PersonHBase p where p.personName = vivek1 OR p.age= 10");

        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(1, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNotNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());

        node.close();
        emLucene.close();
        emfLucene.close();
    }
    
    @Test
    public void deleteTest() throws Exception{
        String id = "commonId";
       
        PersonHBase p = new PersonHBase();
        p.setPersonId(id);
        p.setAge(20);
        p.setDay(Day.MONDAY);
        p.setMonth(Month.JAN);
        p.setPersonName("pragalbh");
        em.persist(p);
        
        Categories cat = new Categories();
        cat.setCategoryId(id);
        cat.setCategoryName("category");
        em.persist(cat);
        em.remove(p);
        
        Categories catTest = em.find(Categories.class, id);
        Assert.assertNotNull(catTest);
        PersonHBase pTest = em.find(PersonHBase.class, id);
        Assert.assertNull(pTest);
        
        em.remove(cat);
        catTest = em.find(Categories.class, id);
        Assert.assertNull(catTest);
    }

    private void init()
    {
        cli.startCluster();
        // cli.createTable("PERSON");
        // cli.addColumnFamily("PERSON", "PERSON");
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

    @Test
    public void onFilterOperation()
    {
        init();

        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get("hbaseTest");

        Filter f/* = new QualifierFilter() */;
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
        if (cli != null)
        {
            cli.dropTable("KunderaExamples");
            cli.stopCluster("KunderaExamples");
        }
        LuceneCleanupUtilities.cleanLuceneDirectory(((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance()
                .getApplicationMetadata().getPersistenceUnitMetadata("hbaseTest"));
        LuceneCleanupUtilities.cleanDir("./lucene");
        emf.close();
        // if (cli.isStarted)

    }
}