/**
 * 
 */
package com.impetus.client.es;

/**
 * @author amitkumar
 *
 */

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.es.PersonES.Day;

public class ESAggregationTest {

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    private static Node node = null;

    private PersonES person;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        if (!checkIfServerRunning()) {
            ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
            builder.put("path.data", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }
    }

    /**
     * Check if server running.
     * 
     * @return true, if successful
     */
    private static boolean checkIfServerRunning() {
        try {
            Socket socket = new Socket("127.0.0.1", 9300);
            return socket.getInetAddress() != null;
        } catch (UnknownHostException e) {
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    @Before
    public void setup() throws InterruptedException {

        emf = Persistence.createEntityManagerFactory("es-pu");
        em = emf.createEntityManager();
        init();
    }

    /**
     * @throws InterruptedException
     */
    private void init() throws InterruptedException {

        createPerson("1", 20, "Amit", 100.0);
        createPerson("2", 10, "Dev", 200.0);
        createPerson("3", 30, "Karthik", 300.0);
        createPerson("4", 40, "Pragalbh", 400.0);
        waitThread();
    }


    /**
     * @param id
     * @param age
     * @param name
     * @param salary
     */
    private void createPerson(String id, int age, String name, Double salary) {
        person = new PersonES();
        person.setAge(age);
        person.setDay(Day.FRIDAY);
        person.setPersonId(id);
        person.setPersonName(name);
        person.setSalary(salary);
        em.persist(person);
    }

    @Test
    public void testMinAggregation() {
        String queryString = "Select min(p.salary) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
    }

    @Test
    public void testMultiMinAggregation() {

        String queryString = "Select min(p.salary), min(p.age) from PersonES p where p.age > 20";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(300.0, resultList.get(0));
        Assert.assertEquals(30.0, resultList.get(1));
    }

    @Test
    public void testMaxAggregation() {
        String queryString = "Select max(p.age) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
    }

    @Test
    public void testMultiMaxAggregation()

    {
        String queryString = "Select max(p.age), max(p.salary) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
    }

    @Test
    public void testSumAggregation() {
        String queryString = "Select sum(p.salary) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(1000.0, resultList.get(0));
    }

    @Test
    public void testMultiSumAggregation() {
        String queryString = "Select sum(p.salary), sum(p.age) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(1000.0, resultList.get(0));
        Assert.assertEquals(100.0, resultList.get(1));
    }

    @Test
    public void testAvgAggregation() {
        String queryString = "Select avg(p.salary) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(250.0, resultList.get(0));
    }

    @Test
    public void testMultiAvgAggregation() {
        String avgQuery = "Select avg(p.salary), avg(p.age) from PersonES p";
        Query query = em.createQuery(avgQuery);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(250.0, resultList.get(0));
        Assert.assertEquals(25.0, resultList.get(1));
    }

    @Test
    public void testMaxMinSameFieldAggregation() {
        String queryString = "Select max(p.age), min(p.age) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
        Assert.assertEquals(10.0, resultList.get(1));
    }

    @Test
    public void testMultiAggregation() {
        String queryString = "Select min(p.age), min(p.salary), max(p.age), max(p.salary) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());
        Assert.assertEquals(10.0, resultList.get(0));
        Assert.assertEquals(100.0, resultList.get(1));
        Assert.assertEquals(40.0, resultList.get(2));
        Assert.assertEquals(400.0, resultList.get(3));
    }

    @Test
    public void testMinMaxSumAvgAggregation() {
        String queryString = "Select min(p.salary), max(p.salary), sum(p.salary), avg(p.salary) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
        Assert.assertEquals(1000.0, resultList.get(2));
        Assert.assertEquals(250.0, resultList.get(3));
    }

    @Test
    public void testAttributeWithMinAggregation() {
        String queryString = "Select p.age, min(p.salary) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40, resultList.get(0));
        Assert.assertEquals(100.0, resultList.get(1));
    }

    @Test
    public void testMinWithAttributeAggregation() {
        String queryString = "Select min(p.salary), p.age from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
        Assert.assertEquals(40, resultList.get(1));
    }

    @Test
    public void testMaxWithAttributeAggregation() {
        String queryString = "Select max(p.salary), p.salary from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(400.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
    }

    @Test
    public void testAttributeWithSumAggregation() {
        String queryString = "Select p.age, sum(p.salary) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40, resultList.get(0));
        Assert.assertEquals(1000.0, resultList.get(1));
    }

    @Test
    public void testSumWithAttributeSameFieldAggregation() {
        String queryString = "Select sum(p.salary), p.salary from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(1000.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
    }

    @Test
    public void testAttributeWithAvgAggregation() {
        String queryString = "Select p.age, avg(p.salary) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40, resultList.get(0));
        Assert.assertEquals(250.0, resultList.get(1));
    }

    @Test
    public void testAvgWithAttributeSameFieldAggregation() {
        String queryString = "Select avg(p.salary), p.salary from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(250.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
    }

    @Test
    public void testMultiFieldAggregation() {
        String queryString =
            "Select p.age, min(p.salary), p.salary, max(p.salary), p.personId, sum(p.salary), p.personName, avg(p.salary) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(8, resultList.size());
        Assert.assertEquals(40, resultList.get(0));
        Assert.assertEquals(100.0, resultList.get(1));
        Assert.assertEquals(400.0, resultList.get(2));
        Assert.assertEquals(400.0, resultList.get(3));
        Assert.assertEquals("4", resultList.get(4));
        Assert.assertEquals(1000.0, resultList.get(5));
        Assert.assertEquals("Pragalbh", resultList.get(6));
        Assert.assertEquals(250.0, resultList.get(7));
    }

    @Test
    public void testAggregationWithWhereClause() throws InterruptedException {
        waitThread();

        String invalidQueryWithAndClause =
            "Select min(p.age) from PersonES p where p.personName = 'amit' AND p.age = 34";
        Query nameQuery = em.createNamedQuery(invalidQueryWithAndClause);
        List persons = nameQuery.getResultList();

        Assert.assertTrue(persons.isEmpty());

        String queryWithAndClause = "Select min(p.age) from PersonES p where p.personName = 'amit' AND p.age = 20";
        nameQuery = em.createNamedQuery(queryWithAndClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(20.0, persons.get(0));

        String queryWithORClause =
            "Select max(p.salary) from PersonES p where p.personName = 'amit' OR p.personName = 'dev'";
        nameQuery = em.createNamedQuery(queryWithORClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(200.0, persons.get(0));

        String queryMinMaxWithORClause =
            "Select max(p.salary), min(p.age) from PersonES p where p.personName = 'amit' OR p.personName = 'karthik'";
        nameQuery = em.createNamedQuery(queryMinMaxWithORClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(2, persons.size());
        Assert.assertEquals(300.0, persons.get(0));
        Assert.assertEquals(20.0, persons.get(1));

        String invalidQueryWithORClause =
            "Select sum(p.age) from PersonES p where p.personName = 'amit' OR p.personName = 'lkl'";
        nameQuery = em.createNamedQuery(invalidQueryWithORClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(20.0, persons.get(0));

        String testSumWithGreaterThanClause = "Select sum(p.age) from PersonES p where p.age > 15";
        nameQuery = em.createNamedQuery(testSumWithGreaterThanClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(90.0, persons.get(0));
        waitThread();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (node != null)
            node.close();
    }

    @After
    public void tearDown() {
        em.remove(em.find(PersonES.class, "1"));
        em.remove(em.find(PersonES.class, "2"));
        em.remove(em.find(PersonES.class, "3"));
        em.remove(em.find(PersonES.class, "4"));
        em.close();
        emf.close();
    }

    /**
     * @throws InterruptedException
     */
    private void waitThread() throws InterruptedException {
        Thread.sleep(2000);
    }
}