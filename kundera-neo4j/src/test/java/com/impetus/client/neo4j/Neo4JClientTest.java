package com.impetus.client.neo4j;


import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestGraphDatabaseFactory;

/**
 * Test case for {@link Neo4JClient} 
 * (See http://docs.neo4j.org/chunked/stable/tutorials-java-unit-testing.html)
 * @author amresh.singh
 */
public class Neo4JClientTest
{
    protected GraphDatabaseService graphDb;

    @Before
    public void prepareTestDatabase()
    {
        graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
    }

    @After
    public void destroyTestDatabase()
    {
        graphDb.shutdown();
    }
    
    @Test
    public void startWithConfiguration()
    {
        // START SNIPPET: startDbWithConfig
        Map<String, String> config = new HashMap<String, String>();
        config.put( "neostore.nodestore.db.mapped_memory", "10M" );
        config.put( "string_block_size", "60" );
        config.put( "array_block_size", "300" );
        GraphDatabaseService db = new ImpermanentGraphDatabase( config );
        // END SNIPPET: startDbWithConfig
        db.shutdown();
    }

    @Test
    public void shouldCreateNode()
    {
        // START SNIPPET: unitTest
        Transaction tx = graphDb.beginTx();

        Node n = null;
        try
        {
            n = graphDb.createNode();
            n.setProperty( "name", "Nancy" );
            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
        }
        finally
        {
            tx.finish();
        }

        // The node should have an id greater than 0, which is the id of the
        // reference node.
        Assert.assertTrue( n.getId() > 0l  );

        // Retrieve a node by using the id of the created node. The id's and
        // property should match.
        Node foundNode = graphDb.getNodeById( n.getId() );
        Assert.assertTrue( foundNode.getId() == n.getId() );
        Assert.assertTrue(((String) foundNode.getProperty( "name" )).equals( "Nancy"));
        // END SNIPPET: unitTest
    }

}
