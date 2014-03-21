/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.query;

import java.util.Calendar;
import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.NoResultException;
import javax.persistence.Persistence;
import javax.persistence.TemporalType;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.CoreTestUtilities;
import com.impetus.kundera.client.DummyDatabase;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * @author vivek.mishra junit for {@link QueryImpl}
 * 
 */
public class QueryImplTest
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

    private KunderaQuery parseQuery(final String query)
    {
        KunderaQuery kunderaQuery = new KunderaQuery(query, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        kunderaQuery.postParsingInit();
        return kunderaQuery;
    }

    @After
    public void tearDown()
    {
        DummyDatabase.INSTANCE.dropDatabase();
        LuceneCleanupUtilities.cleanLuceneDirectory(kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(PU));
    }

    /**
     * @param query
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    @Test
    public void assertOnUnsupportedMethod() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        String queryStr = "Select p from Person p where p.personId = :personId";

        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);

        KunderaQueryParser queryParser;
        KunderaQuery kunderaQuery = parseQuery(queryStr);

        CoreQuery query = new CoreQuery(kunderaQuery, delegator, kunderaMetadata);

        try
        {
            query.setFlushMode(FlushModeType.AUTO);
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("setFlushMode is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setFirstResult(1);
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("setFirstResult is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.getSingleResult();
        }
        catch (NoResultException e)
        {
            Assert.assertEquals("No result found!", e.getMessage());
        }
        
        try
        {
            query.getFirstResult();
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("getFirstResult is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setLockMode(LockModeType.NONE);
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("setLockMode is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.getLockMode();
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("getLockMode is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setParameter(0, new Date(), TemporalType.DATE);
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setParameter("param", new Date(), TemporalType.DATE);
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setParameter(0, Calendar.getInstance(), TemporalType.DATE);
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setParameter("param", Calendar.getInstance(), TemporalType.DATE);
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setParameter(CoreTestUtilities.getParameter(), Calendar.getInstance(), TemporalType.DATE);
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.setParameter(CoreTestUtilities.getParameter(), new Date(), TemporalType.DATE);
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("setParameter is unsupported by Kundera", usex.getMessage());
        }

        try
        {
            query.getFlushMode();
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("getFlushMode is unsupported by Kundera", usex.getMessage());
        }

    }

    @Test
    public void testGetColumns()
    {
        try
        {
            String queryStr = "Select p from Person p where p.personId = :personId";

            PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);

            KunderaQueryParser queryParser;
            KunderaQuery kunderaQuery = parseQuery(queryStr);

            CoreQuery query = new CoreQuery(kunderaQuery, delegator, kunderaMetadata);

            EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, Person.class);
            String[] columns = query.getColumns(new String[] { "personName", "age" }, m);
            Assert.assertNotNull(columns);
            Assert.assertTrue(columns.length > 0);
        }
        catch (SecurityException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (NoSuchFieldException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGroupByAndOrderBy()
    {
        try
        {
            String queryStr = "Select p from Person p where p.personId = :personId GROUP BY personId";
            KunderaQuery kunderaQuery = new KunderaQuery(queryStr, kunderaMetadata);
            KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
            queryParser.parse();
            kunderaQuery.postParsingInit();

            queryStr = "Select p from Person p where p.personId = :personId GROUP BY personId HAVING 1";
            kunderaQuery = new KunderaQuery(queryStr, kunderaMetadata);
            queryParser = new KunderaQueryParser(kunderaQuery);
            queryParser.parse();
            kunderaQuery.postParsingInit();

            queryStr = "Select p from Person p where p.personId = :personId GROUP BY personId ORDER BY personName";
            kunderaQuery = new KunderaQuery(queryStr, kunderaMetadata);
            queryParser = new KunderaQueryParser(kunderaQuery);
            queryParser.parse();
            kunderaQuery.postParsingInit();
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }

    }
}
