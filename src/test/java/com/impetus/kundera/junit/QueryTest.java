/*
 * Copyright 2010 Impetus Infotech.
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
package com.impetus.kundera.junit;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.log4j.Logger;

import com.impetus.kundera.entity.Author;
import com.impetus.kundera.loader.Configuration;

/**
 * The Class QueryTest.
 */
public class QueryTest extends BaseTest
{

    /** The conf. */
    private Configuration conf = new Configuration();

    /** The logger. */
    private static Logger logger = Logger.getLogger(QueryTest.class);

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception
    {
        startCassandraServer();
    }

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception
    {
        conf.destroy();
    }

    /**
     * Test query accross manager sessions.
     * 
     * @throws Exception
     *             the exception
     */
    public void testQueryAccrossManagerSessions() throws Exception
    {
        EntityManager firstManager = getEntityManager();

        int count = 10;
        String msTestRan = Integer.toString(Calendar.getInstance().get(Calendar.MILLISECOND));

        String country = msTestRan + "India";

        for (int i = 0; i < count; i++)
        {
            String key = msTestRan + "-author" + i;
            Author author = createAuthor(key, "someEmail", country, new Date());
            firstManager.persist(author);
        }
        firstManager.close();
        

        EntityManager secondManager = getEntityManager();
        Query q = secondManager.createQuery("select a from Author a where a.country like :country");
        q.setParameter("country", country);

        List<Author> authors = q.getResultList();
        logger.info(authors.size());

        for (Author a : authors)
        {
            secondManager.remove(a);
            logger.info("removing " + a.getUsername());
        }
        // secondManager.close();

        assertEquals(count, authors.size());
    }

    /**
     * Creates the author.
     * 
     * @param username
     *            the username
     * @param email
     *            the email
     * @param country
     *            the country
     * @param registeredSince
     *            the registered since
     * @return the author
     */
    private static Author createAuthor(String username, String email, String country, Date registeredSince)
    {
        Author author = new Author();
        author.setUsername(username);
        author.setCountry(country);
        author.setEmailAddress(email);
        author.setRegistered(registeredSince);
        return author;
    }

    /**
     * Gets the entity manager.
     * 
     * @return the entity manager
     */
    private EntityManager getEntityManager()
    {
        return conf.getEntityManager("cassandra");
    }
}
