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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Parameter;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.KunderaQuery.UpdateClause;

/**
 * @author vivek.mishra Junit for Kundera query test.
 * 
 */
public class KunderaQueryTest
{

    private static final String PU = "patest";

    private EntityManagerFactory emf;

    private EntityManager em;

    private KunderaMetadata kunderaMetadata;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {

        emf = Persistence.createEntityManagerFactory(PU);
        kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
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
        String query = "Select p from Person p";
        KunderaQuery kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        Assert.assertNotNull(kunderaQuery.getEntityClass());
        Assert.assertEquals(Person.class, kunderaQuery.getEntityClass());
        Assert.assertNotNull(kunderaQuery.getEntityMetadata());
        Assert.assertTrue(KunderaMetadataManager.getEntityMetadata(kunderaMetadata, Person.class).equals(
                kunderaQuery.getEntityMetadata()));
        Assert.assertNull(kunderaQuery.getFilter());
        Assert.assertTrue(kunderaQuery.getFilterClauseQueue().isEmpty());
        Assert.assertNotNull(kunderaQuery.getFrom());
        Assert.assertTrue(kunderaQuery.getUpdateClauseQueue().isEmpty());
        Assert.assertNotNull(kunderaQuery.getResult());
        Assert.assertEquals(PU, kunderaQuery.getPersistenceUnit());
        Assert.assertNull(kunderaQuery.getOrdering());
        try
        {
            query = "Select p from p";
            kunderaQuery = new KunderaQuery(query, kunderaMetadata);
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
            query = "Select p form Person p";
            kunderaQuery = new KunderaQuery(query, kunderaMetadata);
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
            query = "Selct p from Person p";
            kunderaQuery = new KunderaQuery(query, kunderaMetadata);
            queryParser = new KunderaQueryParser(kunderaQuery);
            queryParser.parse();
            kunderaQuery.postParsingInit();
            Assert.assertNotNull(kunderaQuery.getEntityClass());
            Assert.assertEquals(Person.class, kunderaQuery.getEntityClass());
            Assert.assertNotNull(kunderaQuery.getEntityMetadata());
            Assert.assertTrue(KunderaMetadataManager.getEntityMetadata(kunderaMetadata, Person.class).equals(
                    kunderaQuery.getEntityMetadata()));
            Assert.assertNull(kunderaQuery.getFilter());
            Assert.assertTrue(kunderaQuery.getFilterClauseQueue().isEmpty());
            Assert.assertNotNull(kunderaQuery.getFrom());
            Assert.assertTrue(kunderaQuery.getUpdateClauseQueue().isEmpty());
            Assert.assertNotNull(kunderaQuery.getResult());
            Assert.assertEquals(PU, kunderaQuery.getPersistenceUnit());
            Assert.assertNull(kunderaQuery.getOrdering());
        }
        catch (JPQLParseException e)
        {
            Assert.fail();
        }
        try
        {
            query = "Select p from Person p where";
            kunderaQuery = new KunderaQuery(query, kunderaMetadata);
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
            query = "Select p from Person p where p";
            kunderaQuery = new KunderaQuery(query, kunderaMetadata);
            queryParser = new KunderaQueryParser(kunderaQuery);
            queryParser.parse();
            kunderaQuery.postParsingInit();
            Assert.fail();
        }
        catch (PersistenceException e)
        {
            Assert.assertEquals("bad jpa query: p", e.getMessage());
        }

        try
        {
            query = "Select p from invalidPerson p";
            kunderaQuery = new KunderaQuery(query, kunderaMetadata);
            queryParser = new KunderaQueryParser(kunderaQuery);
            queryParser.parse();
            kunderaQuery.postParsingInit();
        }
        catch (QueryHandlerException qhex)
        {
            Assert.assertEquals("No entity found by the name: invalidPerson", qhex.getMessage());
        }

        try
        {
            query = "Select p.ABC from Person p";
            kunderaQuery = new KunderaQuery(query, kunderaMetadata);
            queryParser = new KunderaQueryParser(kunderaQuery);
            queryParser.parse();
            kunderaQuery.postParsingInit();
        }
        catch (JPQLParseException e)
        {
            Assert.assertEquals("invalid column nameABC", e.getMessage());
        }

        try
        {
            query = "Select p from Person p order by p.personName ASCENDING";
            kunderaQuery = new KunderaQuery(query, kunderaMetadata);
            queryParser = new KunderaQueryParser(kunderaQuery);
            queryParser.parse();
            kunderaQuery.postParsingInit();
        }
        catch (JPQLParseException e)
        {
            Assert.assertTrue(e.getMessage().startsWith("Invalid sort order provided:ASCENDING"));
        }

    }

    @Test
    public void testOnIndexParameter()
    {
        String query = "Select p from Person p where p.personName = ?1 and p.age= ?2";
        KunderaQuery kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter(1, "pname");
        kunderaQuery.setParameter(2, 32);

        List<String> nameList = new ArrayList<String>();
        List<Integer> ageList = new ArrayList<Integer>();
        ageList.add(32);
        
        Object value = kunderaQuery.getClauseValue("?1");
        nameList.add(value.toString());
        Assert.assertNotNull(value);
        Assert.assertEquals(value, value);
        value = kunderaQuery.getClauseValue("?2");
        Assert.assertNotNull(value);
        Assert.assertEquals(ageList, value);
        Assert.assertEquals(2, kunderaQuery.getParameters().size());

        try
        {
            kunderaQuery.getClauseValue("invalidparam");
            Assert.fail("Should have gone to catch block!");
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertEquals("parameter is not a parameter of the query", iaex.getMessage());
        }

        Assert.assertNotNull(kunderaQuery.getFilterClauseQueue());

        for (Object clause : kunderaQuery.getFilterClauseQueue())
        {
            Assert.assertNotNull(clause);
            Assert.assertNotNull(clause.toString());
            if (clause.getClass().isAssignableFrom(FilterClause.class))
            {
                Assert.assertNotNull(((FilterClause) clause).getProperty());
                Assert.assertNotNull(((FilterClause) clause).getValue().get(0));
                Assert.assertNotNull(((FilterClause) clause).getCondition());
            }
            else
            {
                Assert.assertEquals("AND", clause.toString().trim());
            }
        }

        Iterator<Parameter<?>> parameters = kunderaQuery.getParameters().iterator();

        while (parameters.hasNext())
        {
            Assert.assertTrue(kunderaQuery.isBound(parameters.next()));
        }

        query = "Select p from Person p where p.age between ?1 and ?2 ";
        kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter(1, 32);
        kunderaQuery.setParameter(2, 35);

        value = kunderaQuery.getClauseValue("?1");
        Assert.assertNotNull(value);
        Assert.assertEquals(ageList, value);
        
        ageList = new ArrayList<Integer>();
        ageList.add(35);
        

        
        value = kunderaQuery.getClauseValue("?2");
        Assert.assertNotNull(value);
        Assert.assertEquals(ageList, value);
        Assert.assertEquals(2, kunderaQuery.getParameters().size());

    }

    @Test
    public void testOnNameParameter()
    {
        String query = "Select p from Person p where p.personName = :name and p.age= :age";
        KunderaQuery kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter("name", "pname");
        kunderaQuery.setParameter("age", 32);

        Assert.assertEquals(2, kunderaQuery.getParameters().size());

        Iterator<Parameter<?>> parameters = kunderaQuery.getParameters().iterator();

        while (parameters.hasNext())
        {
            Assert.assertTrue(kunderaQuery.isBound(parameters.next()));
        }
        
        List<String> nameList = new ArrayList<String>();
        nameList.add("pname");
        List<Integer> ageList = new ArrayList<Integer>();
        ageList.add(32);

        Object value = kunderaQuery.getClauseValue(":name");
        Assert.assertNotNull(value);
        Assert.assertEquals(nameList, value);
        value = kunderaQuery.getClauseValue(":age");
        Assert.assertNotNull(value);
        Assert.assertEquals(ageList, value);
        Assert.assertEquals(2, kunderaQuery.getParameters().size());

    }

    @Test
    public void testInvalidIndexParameter()
    {
        String query = "Select p from Person p where p.personName = ?1 and p.age= ?2";
        KunderaQuery kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter(1, "pname");
        kunderaQuery.setParameter(2, 32);

        try
        {
            kunderaQuery.getClauseValue("?3");
            Assert.fail("Should be catch block");
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertEquals("parameter is not a parameter of the query", iaex.getMessage());
        }
    }

    @Test
    public void testInvalidNameParameter()
    {
        String query = "Select p from Person p where p.personName = :name and p.age= :age";
        KunderaQuery kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter("name", "pname");
        kunderaQuery.setParameter("age", 32);

        try
        {
            kunderaQuery.getClauseValue(":naame");
            Assert.fail("Should be catch block");
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertEquals("parameter is not a parameter of the query", iaex.getMessage());
        }
    }

    @Test
    public void testUpdateClause()
    {
        String query = "Update Person p set p.age= ?1 where p.personName = ?2 and p.age = ?3";
        KunderaQuery kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter(1, 33);
        kunderaQuery.setParameter(2, "pname");
        kunderaQuery.setParameter(3, 32);

        Assert.assertEquals(3, kunderaQuery.getParameters().size());

        Iterator<Parameter<?>> parameters = kunderaQuery.getParameters().iterator();

        Assert.assertNotNull(kunderaQuery.getUpdateClauseQueue());

        for (UpdateClause clause : kunderaQuery.getUpdateClauseQueue())
        {
            Assert.assertNotNull(clause);
            Assert.assertNotNull(clause.getProperty());
            Assert.assertNotNull(clause.getValue());
            Assert.assertNotNull(clause.getClass());
            Assert.assertNotNull(clause.toString());
        }

        while (parameters.hasNext())
        {
            Parameter parameter = parameters.next();
            Assert.assertTrue(kunderaQuery.isBound(parameter));
            Assert.assertNull(parameter.getName());
            Assert.assertNotNull(parameter.getPosition());
            Assert.assertNotNull(parameter.toString());
        }

        try
        {
            kunderaQuery.getClauseValue(":naame");
            Assert.fail("Should have gone to catch block!");
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNotNull(kunderaQuery.toString());
            Assert.assertEquals("parameter is not a parameter of the query", iaex.getMessage());
        }

        try
        {
            kunderaQuery.getClauseValue(new JPAParameter());
            Assert.fail("Should have gone to catch block!");
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNotNull(kunderaQuery.toString());
            Assert.assertEquals("parameter is not a parameter of the query", iaex.getMessage());
        }

    }
    
    
    @Test
    public void testWithInClause()
    {
        final String query = "Select p from Person p where p.personName <> :name and p.age in('kk', 'dk', 'sk') ";
        KunderaQuery kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
    }

    @Test
    public void testInNotInNotEquals()
    {
        String query = "Select p from Person p where p.personName <> :name and p.age IN :ageList" +
        		" and salary NOT IN :salaryList and (personId = :personId)";
        KunderaQuery kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter("name", "pname");
        kunderaQuery.setParameter("ageList", new ArrayList<Integer>(){{add(20);add(21);}});
        kunderaQuery.setParameter("salaryList", new ArrayList<Double>(){{add(2000D);add(3000D);}});
        kunderaQuery.setParameter("personId", "personId");

        assertSubQuery(kunderaQuery);

    }

    @Test
    public void testWithSubQueryInMid()
    {
        String query = "Select p from Person p where p.personName <> :name and p.age IN :ageList" +
        " and (personId = :personId) and salary NOT IN :salaryList";
        KunderaQuery kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter("name", "pname");
        kunderaQuery.setParameter("ageList", new ArrayList<Integer>(){{add(20);add(21);}});
        kunderaQuery.setParameter("salaryList", new ArrayList<Double>(){{add(2000D);add(3000D);}});
        kunderaQuery.setParameter("personId", "personId");

        assertSubQuery(kunderaQuery);

    }
    
    @Test
    public void testWithSubQueryInMidWithoutSetParam()
    {
        String query = "Select p from Person p where p.personName <> pname and p.age IN (20,21,32)" +
        " and (personId = :personId) and salary NOT IN :salaryList";
        KunderaQuery kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter("salaryList", new ArrayList<Double>(){{add(2000D);add(3000D);}});
        kunderaQuery.setParameter("personId", "personId");

        assertMixSubQuery(kunderaQuery, 2);
        
        query = "Select p from Person p where p.personName <> in and p.age IN (20,21,32)" +
                " and (personId = :personId) and salary NOT IN :salaryList";
        kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        kunderaQuery.setParameter("salaryList", new ArrayList<Double>(){{add(2000D);add(3000D);}});
        kunderaQuery.setParameter("personId", "personId");
        assertMixSubQuery(kunderaQuery, 2);

    }
    
    
    private void assertMixSubQuery(KunderaQuery kunderaQuery , int paramSize)
    {
        Assert.assertEquals(paramSize, kunderaQuery.getParameters().size());

        Iterator<Parameter<?>> parameters = kunderaQuery.getParameters().iterator();

        while (parameters.hasNext())
        {
            Assert.assertTrue(kunderaQuery.isBound(parameters.next()));
        }
        
        List<String> nameList = new ArrayList<String>();
        nameList.add("pname");
        List<Integer> ageList = new ArrayList<Integer>(){{add(20);add(21);}};
        List<Double> salaryList = new ArrayList<Double>(){{add(2000D);add(3000D);}};
        List<String> personIdList = new ArrayList<String>();
        personIdList.add("personId");

        Object value = kunderaQuery.getClauseValue(":salaryList");
        Assert.assertNotNull(value);
        Assert.assertEquals(salaryList, value);
        value = kunderaQuery.getClauseValue(":personId");
        Assert.assertNotNull(value);
        Assert.assertEquals(personIdList, value);
        Assert.assertEquals(paramSize, kunderaQuery.getParameters().size());
    }


    private void assertSubQuery(KunderaQuery kunderaQuery)
    {
        Assert.assertEquals(4, kunderaQuery.getParameters().size());

        Iterator<Parameter<?>> parameters = kunderaQuery.getParameters().iterator();

        while (parameters.hasNext())
        {
            Assert.assertTrue(kunderaQuery.isBound(parameters.next()));
        }
        
        List<String> nameList = new ArrayList<String>();
        nameList.add("pname");
        List<Integer> ageList = new ArrayList<Integer>(){{add(20);add(21);}};
        List<Double> salaryList = new ArrayList<Double>(){{add(2000D);add(3000D);}};
        List<String> personIdList = new ArrayList<String>();
        personIdList.add("personId");

        Object value = kunderaQuery.getClauseValue(":name");
        Assert.assertNotNull(value);
        Assert.assertEquals(nameList, value);
        value = kunderaQuery.getClauseValue(":ageList");
        Assert.assertNotNull(value);
        Assert.assertEquals(ageList, value);
        value = kunderaQuery.getClauseValue(":salaryList");
        Assert.assertNotNull(value);
        Assert.assertEquals(salaryList, value);
        value = kunderaQuery.getClauseValue(":personId");
        Assert.assertNotNull(value);
        Assert.assertEquals(personIdList, value);
        Assert.assertEquals(4, kunderaQuery.getParameters().size());
    }

    private class JPAParameter implements Parameter<String>
    {

        @Override
        public String getName()
        {
            return "jpa";
        }

        @Override
        public Integer getPosition()
        {
            return 0;
        }

        @Override
        public Class<String> getParameterType()
        {
            return String.class;
        }

    }
}
