package com.impetus.client.oraclenosql.crud;

import javax.persistence.Persistence;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.client.query.GroupByBaseTest;

/**
 * The Class OracleNoSqlESGroupByTest.
 * 
 * @author karthikp.manchala
 */
public class OracleNoSqlESGroupByTest extends GroupByBaseTest
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
            ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
            builder.put("path.data", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }

        emf = Persistence.createEntityManagerFactory("ESkvstore");
        em = emf.createEntityManager();
        init();
    }

    /**
     * Test aggregation.
     */
    @Test
    public void test()
    {
        testAggregation();
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
        em.createQuery("Delete from Person p").executeUpdate();
        waitThread();

        em.close();
        emf.close();

        if (node != null)
            node.close();
    }
}