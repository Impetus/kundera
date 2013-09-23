package com.impetus.client.crud;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

public class EntityWithClauseBatchTest
{

    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void test()
    {
        Map<String, String> propertyMap = new HashMap<String, String>();
        propertyMap.put("kundera.batch.size", "5");

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("cassandra_pu", propertyMap);

        propertyMap.put("kundera.batch.size", "10");

        EntityManager em = emf.createEntityManager(propertyMap);

        for (int i = 1; i <= 10; i++)
        {
            EntityWithClause entityWithClause1 = new EntityWithClause("1" + i);
            entityWithClause1.setIncome("1233");
            entityWithClause1.setLikedBy("kk");
            entityWithClause1.setSettlementDate("12/12/12");
            entityWithClause1.setAnd("and");
            entityWithClause1.setBetween("between");
            entityWithClause1.setDateSet("dateSet");
            entityWithClause1.setOr("or");

            em.persist(entityWithClause1);

            if (i == 10)
            {
                List<EntityWithClause> entityWithClauses = em.createQuery("select t from EntityWithClause t")
                        .getResultList();
                Assert.assertNotNull(entityWithClauses);
                Assert.assertEquals(10, entityWithClauses.size());
            }
            else
            {
                List<EntityWithClause> entityWithClauses = em.createQuery("select t from EntityWithClause t")
                        .getResultList();
                Assert.assertNotNull(entityWithClauses);
                Assert.assertTrue(entityWithClauses.isEmpty());
            }
        }
    }
}