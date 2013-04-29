package com.impetus.client.crud;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

public class OTMCRUDTest
{

    private static final String SEC_IDX_CASSANDRA_TEST = "myapp_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        CassandraCli.createKeySpace("myapp");
        emf = Persistence.createEntityManagerFactory(SEC_IDX_CASSANDRA_TEST);
        em = emf.createEntityManager();
    }

    @After
    public void tearDown() throws Exception
    {
        String query = "Delete t from Token t";
        Query q = em.createQuery(query);
        q.executeUpdate();

        if (emf != null)
        {
            emf.close();
            emf = null;
        }
        if (em != null)
        {
            em.close();
            em = null;
        }

        CassandraCli.dropKeySpace("myapp");
    }

    @Test
    public void test()
    {
        Token token1 = new Token();
        token1.setId("tokenId1");
        TokenClient client = new TokenClient();
        client.setClientName("tokenClient1");
        client.setId("tokenClientId");
        token1.setClient(client);

        Token token2 = new Token();
        token2.setId("tokenId2");
        token2.setClient(client);
        em.persist(token1);
        em.persist(token2);

        em.clear();
        Token result = em.find(Token.class, "tokenId1");

        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getClient());

        em.clear();
        String query = "Select t from Token t";
        Query q = em.createQuery(query);
        List<Token> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertNotNull(results.get(0).getClient());
        Assert.assertNotNull(results.get(1).getClient());

        em.clear();
        query = "Select t from TokenClient t";
        q = em.createQuery(query);
        List<TokenClient> resultClient = q.getResultList();
        Assert.assertNotNull(resultClient);
        Assert.assertEquals(1, resultClient.size());
        Assert.assertEquals(2, resultClient.get(0).getTokens().size());

    }

}
