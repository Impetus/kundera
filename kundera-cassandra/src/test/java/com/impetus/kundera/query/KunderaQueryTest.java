/**
 * 
 */
package com.impetus.kundera.query;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.entity.CassandraUUIDEntity;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.metadata.KunderaMetadataManager;

/**
 * @author impadmin
 * 
 */
public class KunderaQueryTest
{

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("UUIDCassandra");
        emf = Persistence.createEntityManagerFactory("cass_pu");
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
    }

    @Test
    public void test()
    {
        String query = "Select p from CassandraUUIDEntity p";
        KunderaQuery kunderaQuery = new KunderaQuery();
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery, query);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        Assert.assertNotNull(kunderaQuery.getEntityClass());
        Assert.assertEquals(CassandraUUIDEntity.class, kunderaQuery.getEntityClass());
        Assert.assertNotNull(kunderaQuery.getEntityMetadata());
        Assert.assertTrue(KunderaMetadataManager.getEntityMetadata(CassandraUUIDEntity.class).equals(
                kunderaQuery.getEntityMetadata()));
        Assert.assertNull(kunderaQuery.getFilter());
        Assert.assertTrue(kunderaQuery.getFilterClauseQueue().isEmpty());
        Assert.assertNotNull(kunderaQuery.getFrom());
        Assert.assertTrue(kunderaQuery.getUpdateClauseQueue().isEmpty());
        Assert.assertNotNull(kunderaQuery.getResult());
        Assert.assertEquals("cass_pu", kunderaQuery.getPersistenceUnit());
        Assert.assertNull(kunderaQuery.getOrdering());
        try
        {
            query = "Select p from p";
            kunderaQuery = new KunderaQuery();
            queryParser = new KunderaQueryParser(kunderaQuery, query);
            queryParser.parse();
            kunderaQuery.postParsingInit();
            Assert.fail();
        }
        catch (JPQLParseException e)
        {
            Assert.assertEquals(
                    "Bad query format: p. Identification variable is mandatory in FROM clause for SELECT queries. For details, see: http://openjpa.apache.org/builds/1.0.4/apache-openjpa-1.0.4/docs/manual/jpa_langref.html#jpa_langref_bnf",
                    e.getMessage());
        }
        try
        {
            query = "Select p form CassandraUUIDEntity p";
            kunderaQuery = new KunderaQuery();
            queryParser = new KunderaQueryParser(kunderaQuery, query);
            queryParser.parse();
            kunderaQuery.postParsingInit();
            Assert.fail();
        }
        catch (JPQLParseException e)
        {
            Assert.assertEquals(
                    "Bad query format FROM clause is mandatory for SELECT queries. For details, see: http://openjpa.apache.org/builds/1.0.4/apache-openjpa-1.0.4/docs/manual/jpa_langref.html#jpa_langref_bnf",
                    e.getMessage());
        }
        try
        {
            query = "Selct p from CassandraUUIDEntity p";
            kunderaQuery = new KunderaQuery();
            queryParser = new KunderaQueryParser(kunderaQuery, query);
            queryParser.parse();
            kunderaQuery.postParsingInit();
            Assert.assertNotNull(kunderaQuery.getEntityClass());
            Assert.assertEquals(CassandraUUIDEntity.class, kunderaQuery.getEntityClass());
            Assert.assertNotNull(kunderaQuery.getEntityMetadata());
            Assert.assertTrue(KunderaMetadataManager.getEntityMetadata(CassandraUUIDEntity.class).equals(
                    kunderaQuery.getEntityMetadata()));
            Assert.assertNull(kunderaQuery.getFilter());
            Assert.assertTrue(kunderaQuery.getFilterClauseQueue().isEmpty());
            Assert.assertNotNull(kunderaQuery.getFrom());
            Assert.assertTrue(kunderaQuery.getUpdateClauseQueue().isEmpty());
            Assert.assertNotNull(kunderaQuery.getResult());
            Assert.assertEquals("cass_pu", kunderaQuery.getPersistenceUnit());
            Assert.assertNull(kunderaQuery.getOrdering());
        }
        catch (JPQLParseException e)
        {
            Assert.fail();
        }
        try
        {
            query = "Select p from CassandraUUIDEntity p where";
            kunderaQuery = new KunderaQuery();
            queryParser = new KunderaQueryParser(kunderaQuery, query);
            queryParser.parse();
            kunderaQuery.postParsingInit();
            Assert.fail();
        }
        catch (JPQLParseException e)
        {
            Assert.assertEquals(
                    "keyword without value[WHERE]. For details, see: http://openjpa.apache.org/builds/1.0.4/apache-openjpa-1.0.4/docs/manual/jpa_langref.html#jpa_langref_bnf",
                    e.getMessage());
        }
        try
        {
            query = "Select p from CassandraUUIDEntity p where p";
            kunderaQuery = new KunderaQuery();
            queryParser = new KunderaQueryParser(kunderaQuery, query);
            queryParser.parse();
            kunderaQuery.postParsingInit();
            Assert.fail();
        }
        catch (PersistenceException e)
        {
            Assert.assertEquals("bad jpa query: p", e.getMessage());
        }
    }
}
