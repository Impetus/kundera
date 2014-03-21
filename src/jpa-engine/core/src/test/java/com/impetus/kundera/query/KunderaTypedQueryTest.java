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
import javax.persistence.TypedQuery;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.entities.SampleEntity;

/**
 * @author vivek.mishra
 * 
 *         junit for {@link KunderaTypedQuery}
 * 
 */
public class KunderaTypedQueryTest
{

    private EntityManager em;

    private EntityManagerFactory emf;

    @Before
    public void setUp()
    {

        emf = Persistence.createEntityManagerFactory("kunderatest");

        em = emf.createEntityManager();
    }

    @Test
    public void testTypedQuery()
    {
        final String namedQuery = "Select s from SampleEntity s where s.name = :name";

        TypedQuery<SampleEntity> query = em.createNamedQuery(namedQuery, SampleEntity.class);

        Assert.assertTrue(query.getClass().isAssignableFrom(KunderaTypedQuery.class));

        query.setMaxResults(100);
        Assert.assertEquals(100, query.getMaxResults());

        Assert.assertEquals(0, query.executeUpdate());

        Assert.assertEquals(0, query.getHints().size());
        query.setHint("test", "test");
        Assert.assertNotNull(query.getHints());

        assertOnUnsupportedMethod(query);
        Assert.assertEquals(FlushModeType.AUTO, FlushModeType.AUTO);

        ((Query) query).setFetchSize(100);
        Assert.assertNotNull(((Query) query).getFetchSize());
        Assert.assertEquals(100, ((Query) query).getFetchSize().intValue());

        Assert.assertNotNull(query.getParameter("name"));

        Assert.assertNotNull(query.getParameterValue("name"));

        Assert.assertNotNull(query.getParameterValue(query.getParameter("name")));
        Assert.assertEquals(1, query.getParameters().size());
        Assert.assertTrue(query.isBound(query.getParameter("name")));

        Assert.assertNull(query.getParameter(1));

        Assert.assertNull(((Query) query).iterate());

        try
        {
            query.getSingleResult();
        }
        catch (NoResultException e)
        {
            Assert.assertEquals("No result found!", e.getMessage());
        }
        ((Query) query).close();
    }

    /**
     * @param query
     */
    private void assertOnUnsupportedMethod(TypedQuery<SampleEntity> query)
    {
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

        // try
        // {
        // query.getSingleResult();
        // } catch(UnsupportedOperationException usex)
        // {
        // Assert.assertEquals("getSingleResult is unsupported by Kundera",
        // usex.getMessage());
        // }

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
            query.getFlushMode();
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertEquals("getFlushMode is unsupported by Kundera", usex.getMessage());
        }

    }

}
