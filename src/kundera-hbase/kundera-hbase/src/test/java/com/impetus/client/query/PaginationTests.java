/**
 * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.query;

import java.util.Iterator;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.crud.BaseTest;
import com.impetus.client.hbase.crud.PersonHBase;
import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.query.IResultIterator;
import com.impetus.kundera.query.Query;

/**
 * @author Pragalbh Garg junit for {@link IResultIterator}.
 */
public class PaginationTests extends BaseTest
{
    private static final int NO_OF_RECORDS = 1000;

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    private HBaseCli cli = new HBaseCli();

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        cli.startCluster();
        emf = Persistence.createEntityManagerFactory("hbaseTest");
        em = emf.createEntityManager();
        persistData(NO_OF_RECORDS);
    }

    @After
    public void tearDown()
    {
        cli.stopCluster("PERSON_HBASE");
    }

    @Test
    public void chunkScrollTest() throws Exception
    {
        int chunkSize = 50;
        Query query = (Query) em.createQuery("Select p from PersonHBase p", PersonHBase.class);
        query.setFetchSize(1000);
        Iterator<PersonHBase> iter = query.iterate();
        IResultIterator<PersonHBase> iIter = (IResultIterator<PersonHBase>) iter;
        while (iIter.hasNext())
        {
            List<PersonHBase> result = iIter.next(chunkSize);
            Assert.assertNotNull(result);
            Assert.assertEquals(50, result.size());
        }
    }

    private void persistData(int noOfRecords)
    {
        while (noOfRecords--!= 0)
        {
            Object p = prepareHbaseInstance("" + noOfRecords, 20);
            em.persist(p);
            em.clear();
        }
    }
}