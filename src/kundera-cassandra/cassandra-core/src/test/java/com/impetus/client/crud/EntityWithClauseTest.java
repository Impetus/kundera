/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.crud;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.query.JPQLParseException;

/**
 * Junit to test entity with various where clauses.
 * 
 * @author impetus
 *
 */
public class EntityWithClauseTest
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

    @org.junit.Test
    public void test()
    {
        Map<String, String> propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("cassandra_pu", propertyMap);
        EntityManager em = emf.createEntityManager();

        EntityWithClause entityWithClause1 = new EntityWithClause("1");
        entityWithClause1.setIncome("1233");
        entityWithClause1.setLikedBy("kk");
        entityWithClause1.setSettlementDate("12/12/12");
        entityWithClause1.setAnd("and");
        entityWithClause1.setBetween("between");
        entityWithClause1.setDateSet("dateSet");
        entityWithClause1.setOr("or");
        entityWithClause1.setSet("set");

        EntityWithClause entityWithClause2 = new EntityWithClause("2");
        entityWithClause2.setIncome("2233");
        entityWithClause2.setLikedBy("kk");
        entityWithClause2.setSettlementDate("12/12/12");
        entityWithClause2.setAnd("and");
        entityWithClause2.setBetween("between");
        entityWithClause2.setDateSet("dateSet");
        entityWithClause2.setOr("or");
        entityWithClause2.setSet("set");

        EntityWithClause entityWithClause3 = new EntityWithClause("3");
        entityWithClause3.setIncome("3233");
        entityWithClause3.setLikedBy("kk");
        entityWithClause3.setSettlementDate("12/12/13");
        entityWithClause3.setAnd("and");
        entityWithClause3.setBetween("between");
        entityWithClause3.setDateSet("dateSet");
        entityWithClause3.setOr("or");

        em.persist(entityWithClause1);
        em.persist(entityWithClause2);
        em.persist(entityWithClause3);

        em.clear();

        // Select query.
        List<EntityWithClause> entityWithClauses = em.createQuery(
                "select t from EntityWithClause t where t.income=1233").getResultList();
        Assert.assertNotNull(entityWithClauses);
        Assert.assertEquals(1, entityWithClauses.size());

        entityWithClauses = em.createQuery("select t from EntityWithClause t where t.likedBy=kk").getResultList();
        Assert.assertNotNull(entityWithClauses);
        Assert.assertEquals(3, entityWithClauses.size());

        entityWithClauses = em.createQuery("select t from EntityWithClause t where t.dateSet=dateSet").getResultList();
        Assert.assertNotNull(entityWithClauses);
        Assert.assertEquals(3, entityWithClauses.size());

        entityWithClauses = em.createQuery("select t from EntityWithClause t where t.settlementDate=12/12/12")
                .getResultList();
        Assert.assertNotNull(entityWithClauses);
        Assert.assertEquals(2, entityWithClauses.size());

//        entityWithClauses = em.createQuery("select t from EntityWithClause t where t.or=or").getResultList();
//        Assert.assertNotNull(entityWithClauses);
//        Assert.assertEquals(3, entityWithClauses.size());

//        entityWithClauses = em.createQuery("select t from EntityWithClause t where t.between=between").getResultList();
//        Assert.assertNotNull(entityWithClauses);
//        Assert.assertEquals(3, entityWithClauses.size());
//
//        entityWithClauses = em.createQuery("select t from EntityWithClause t where t.and=and").getResultList();
//        Assert.assertNotNull(entityWithClauses);
//        Assert.assertEquals(3, entityWithClauses.size());

        try
        {
            entityWithClauses = em.createQuery("select set from EntityWithClause set where set.set=set")
                    .getResultList();
            Assert.fail();
        }
        catch (JPQLParseException e)
        {
            Assert.assertEquals(
                    "Out of order keyword set, entity alias must not be any reserved keyword.. For details, see: http://openjpa.apache.org/builds/1.0.4/apache-openjpa-1.0.4/docs/manual/jpa_langref.html#jpa_langref_bnf",
                    e.getMessage());
        }

        try
        {
            entityWithClauses = em.createQuery("select from from EntityWithClause from where from.set=set")
                    .getResultList();
            Assert.fail();
        }
        catch (JPQLParseException e)
        {
            Assert.assertEquals(
                    "Out of order keyword from, entity alias must not be any reserved keyword.. For details, see: http://openjpa.apache.org/builds/1.0.4/apache-openjpa-1.0.4/docs/manual/jpa_langref.html#jpa_langref_bnf",
                    e.getMessage());
        }

        // Update query.
        int affectedRecord = em.createQuery("Update EntityWithClause t SET t.or=newor where t.id = 1").executeUpdate();
        EntityWithClause result = em.find(EntityWithClause.class, "1");
        Assert.assertNotNull(result);
        Assert.assertEquals("newor", result.getOr());

        em.clear();

        affectedRecord = em.createQuery("Update EntityWithClause t SET t.and=newand where t.id = 1").executeUpdate();
        result = em.find(EntityWithClause.class, "1");
        Assert.assertNotNull(result);
        Assert.assertEquals("newand", result.getAnd());

        em.clear();

        affectedRecord = em.createQuery("Update EntityWithClause t SET t.set=newset where t.id = 1").executeUpdate();
        result = em.find(EntityWithClause.class, "1");
        Assert.assertNotNull(result);
        Assert.assertEquals("newset", result.getSet());

        em.clear();

        affectedRecord = em.createQuery("Update EntityWithClause t SET t.between=newbetween where t.id = 1")
                .executeUpdate();
        result = em.find(EntityWithClause.class, "1");
        Assert.assertNotNull(result);
        Assert.assertEquals("newbetween", result.getBetween());

        em.clear();

        affectedRecord = em.createQuery("Update EntityWithClause t SET t.likedBy=newKK where t.id = 1").executeUpdate();
        result = em.find(EntityWithClause.class, "1");
        Assert.assertNotNull(result);
        Assert.assertEquals("newKK", result.getLikedBy());

        try
        {
            affectedRecord = em.createQuery("Update EntityWithClause set SET set.likedBy=newKK where set.id = 1")
                    .executeUpdate();
            Assert.fail();
        }
        catch (JPQLParseException e)
        {
            Assert.assertEquals(
                    "Out of order keyword SET, entity alias must not be any reserved keyword.. For details, see: http://openjpa.apache.org/builds/1.0.4/apache-openjpa-1.0.4/docs/manual/jpa_langref.html#jpa_langref_bnf",
                    e.getMessage());
        }

        try
        {
            affectedRecord = em.createQuery("Update EntityWithClause from SET from.likedBy=newKK where from.id = 1")
                    .executeUpdate();
            Assert.fail();
        }
        catch (JPQLParseException e)
        {
            Assert.assertEquals(
                    "Out of order keyword from, entity alias must not be any reserved keyword.. For details, see: http://openjpa.apache.org/builds/1.0.4/apache-openjpa-1.0.4/docs/manual/jpa_langref.html#jpa_langref_bnf",
                    e.getMessage());
        }

        // Delete query.

//        affectedRecord = em.createQuery("Delete From EntityWithClause t where t.or=newor").executeUpdate();
//        result = em.find(EntityWithClause.class, "1");
//        Assert.assertNull(result);

//        try
//        {
//            affectedRecord = em.createQuery("Delete From EntityWithClause t where t.and=and").executeUpdate();
//            result = em.find(EntityWithClause.class, "2");
//            Assert.assertNull(result);
//        }
//        catch (JPQLParseException e)
//        {
//            Assert.assertEquals(
//                    "Out of order keyword from, entity alias must not be any reserver keyword.. For details, see: http://openjpa.apache.org/builds/1.0.4/apache-openjpa-1.0.4/docs/manual/jpa_langref.html#jpa_langref_bnf",
//                    e.getMessage());
//        }

    }
}