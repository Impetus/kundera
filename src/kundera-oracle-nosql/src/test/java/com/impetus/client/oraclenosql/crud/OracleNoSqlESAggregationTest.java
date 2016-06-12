package com.impetus.client.oraclenosql.crud;

import java.util.List;

import javax.persistence.Persistence;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.client.query.AggregationsBaseTest;
import com.impetus.kundera.query.Person;

import junit.framework.Assert;

/**
 * The Class OracleNoSqlESAggregationTest.
 * 
 * @author karthikp.manchala
 */
public class OracleNoSqlESAggregationTest extends AggregationsBaseTest
{
    /** The node. */
    private static Node node = null;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        if (!checkIfServerRunning())
        {
            Builder builder = Settings.settingsBuilder();
            builder.put("path.home", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }
    }

    /**
     * Setup.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    @Before
    public void setup() throws InterruptedException
    {

        emf = Persistence.createEntityManagerFactory("ESkvstore");
        em = emf.createEntityManager();
        init();
    }
    
    @Test
    public void aggregationTest(){
        testAggregation();
    }
    
    @Test
    public void indexDeletionTest() throws Exception
    {
        Thread.sleep(1000);
        String query = "Select min(p.salary) from Person p";
        List resultList = em.createQuery(query).getResultList();
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
        
        Person person = em.find(Person.class, "1");
        em.remove(person);
        Thread.sleep(1000);
        query = "Select min(p.salary) from Person p";
        resultList = em.createQuery(query).getResultList();
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(200.0, resultList.get(0));
        
        Person person2 = em.find(Person.class, "2");
        em.remove(person2);
        Thread.sleep(1000);
        query = "Select min(p.salary) from Person p";
        resultList = em.createQuery(query).getResultList();
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(300.0, resultList.get(0));
        
        em.persist(person);
        em.persist(person2);
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
        if (node != null)
            node.close();
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown()
    {
        em.remove(em.find(Person.class, "1"));
        em.remove(em.find(Person.class, "2"));
        em.remove(em.find(Person.class, "3"));
        em.remove(em.find(Person.class, "4"));
        em.close();
        emf.close();
    }
}