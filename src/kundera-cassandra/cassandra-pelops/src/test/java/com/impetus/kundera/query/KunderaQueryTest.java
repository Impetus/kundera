/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.query;

import java.util.Iterator;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Parameter;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.cassandra.pelops.crud.CassandraUUIDEntity;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.query.JPQLParseException;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQueryParser;

/**
 * @author kuldeep.mishra
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
        KunderaQuery kunderaQuery = new KunderaQuery(query, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        Assert.assertNotNull(kunderaQuery.getEntityClass());
        Assert.assertEquals(CassandraUUIDEntity.class, kunderaQuery.getEntityClass());
        Assert.assertNotNull(kunderaQuery.getEntityMetadata());
        Assert.assertTrue(KunderaMetadataManager.getEntityMetadata(((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance(), CassandraUUIDEntity.class).equals(
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
            kunderaQuery = new KunderaQuery(query, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
            queryParser = new KunderaQueryParser(kunderaQuery);
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
            kunderaQuery = new KunderaQuery(query, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
            queryParser = new KunderaQueryParser(kunderaQuery);
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
            kunderaQuery = new KunderaQuery(query, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
            queryParser = new KunderaQueryParser(kunderaQuery);
            queryParser.parse();
            kunderaQuery.postParsingInit();
            Assert.assertNotNull(kunderaQuery.getEntityClass());
            Assert.assertEquals(CassandraUUIDEntity.class, kunderaQuery.getEntityClass());
            Assert.assertNotNull(kunderaQuery.getEntityMetadata());
            Assert.assertTrue(KunderaMetadataManager.getEntityMetadata(((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance(), CassandraUUIDEntity.class).equals(
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
            kunderaQuery = new KunderaQuery(query, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
            queryParser = new KunderaQueryParser(kunderaQuery);
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
            kunderaQuery = new KunderaQuery(query, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
            queryParser = new KunderaQueryParser(kunderaQuery);
            queryParser.parse();
            kunderaQuery.postParsingInit();
            Assert.fail();
        }
        catch (PersistenceException e)
        {
            Assert.assertEquals("bad jpa query: p", e.getMessage());
        }
    }

    @Test
    public void testOnIndexParameter()
    {
        String query = "Select p from CassandraUUIDEntity p where p.uuidKey = ?1 and p.name= ?2";
        KunderaQuery kunderaQuery = new KunderaQuery(query, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter(1, "uuid1");
        kunderaQuery.setParameter(2, "uuidname");
        
        Object value = kunderaQuery.getClauseValue("?1");
        Assert.assertNotNull(value);
        Assert.assertEquals("uuid1", value);
        value = kunderaQuery.getClauseValue("?2");
        Assert.assertNotNull(value);
        Assert.assertEquals("uuidname", value);
        Assert.assertEquals(2, kunderaQuery.getParameters().size());
        
        Iterator<Parameter<?>> parameters = kunderaQuery.getParameters().iterator();
        
        while(parameters.hasNext())
        {
            Assert.assertTrue(kunderaQuery.isBound(parameters.next()));
        }
    }
    
    @Test
    public void testOnNameParameter()
    {
        String query = "Select p from CassandraUUIDEntity p where p.uuidKey = :uuid and p.name= :name";
        KunderaQuery kunderaQuery = new KunderaQuery(query, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter("uuid", "uuid1");
        kunderaQuery.setParameter("name", "uuidname");
        
        Assert.assertEquals(2, kunderaQuery.getParameters().size());
        
        Iterator<Parameter<?>> parameters = kunderaQuery.getParameters().iterator();
        
        while(parameters.hasNext())
        {
            Assert.assertTrue(kunderaQuery.isBound(parameters.next()));
        }

        Object value = kunderaQuery.getClauseValue(":uuid");
        Assert.assertNotNull(value);
        Assert.assertEquals("uuid1", value);
        value = kunderaQuery.getClauseValue(":name");
        Assert.assertNotNull(value);
        Assert.assertEquals("uuidname", value);
        Assert.assertEquals(2, kunderaQuery.getParameters().size());

    }

    @Test
    public void testInvalidIndexParameter()
    {
        String query = "Select p from CassandraUUIDEntity p where p.uuidKey = ?1 and p.name= ?2";
        KunderaQuery kunderaQuery = new KunderaQuery(query, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter(1, "uuid1");
        kunderaQuery.setParameter(2, "uuidname");
        
        try
        {
            kunderaQuery.getClauseValue("?3");
            Assert.fail("Should be catch block");
        } catch(IllegalArgumentException iaex)
        {
            Assert.assertEquals("parameter is not a parameter of the query", iaex.getMessage());
        }
    }
    
    @Test
    public void testInvalidNameParameter()
    {
        String query = "Select p from CassandraUUIDEntity p where p.uuidKey = :uuid and p.name= :name";
        KunderaQuery kunderaQuery = new KunderaQuery(query, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter("uuid", "uuid1");
        kunderaQuery.setParameter("name", "uuidname");
        
        try
        {
            kunderaQuery.getClauseValue(":naame");
            Assert.fail("Should be catch block");
        } catch(IllegalArgumentException iaex)
        {
            Assert.assertEquals("parameter is not a parameter of the query", iaex.getMessage());
        }
    }

}
