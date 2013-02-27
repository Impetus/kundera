/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.loader.KunderaAuthenticationException;

/**
 * The Class MongoAuthenticationTest.
 * 
 * @author vivek.mishra
 */
public class MongoAuthenticationTest extends BaseTest
{
    private EntityManagerFactory emf;

    private String pu;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        // do nothing.
    }

    /**
     * Authenticate with valid credentials.
     */
    @Test
    public void authenticateWithValidCredentials()
    {
        try
        {
            pu = "validAuthenticationMongoPu";
            emf = Persistence.createEntityManagerFactory(pu);
            Assert.assertNotNull(emf);
            EntityManager em = emf.createEntityManager();
            Assert.assertNotNull(em);
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }

    }

    /**
     * Authenticate with invalid credentials.
     */
    @Test
    public void authenticateWithInValidCredentials()
    {
        EntityManager em = null;
        try
        {
            pu = "invalidAuthenticationMongoPu";
            emf = Persistence.createEntityManagerFactory(pu);
            em = emf.createEntityManager();
            Assert.fail("Shouldn't be called");
        }
        catch (KunderaAuthenticationException e)
        {
            Assert.assertNull(emf);
            Assert.assertNull(em);
        }

    }

    /**
     * No authentication test.
     * 
     */
    @Test
    public void noAuthenticationTest()
    {
        try
        {
            pu = "mongoTest";
            emf = Persistence.createEntityManagerFactory(pu);
            Assert.assertNotNull(emf);
            EntityManager em = emf.createEntityManager();
            Assert.assertNotNull(em);
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }

    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
//        MongoUtils.dropDatabase(emf, pu);
    }
}
