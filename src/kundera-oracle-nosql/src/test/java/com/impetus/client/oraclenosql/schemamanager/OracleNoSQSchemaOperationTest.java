/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.oraclenosql.schemamanager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.PersistenceProperties;

/**
 * The Class OracleNoSQSchemaOperationTest.
 * 
 * @author devender.yadav
 */
public class OracleNoSQSchemaOperationTest
{
    /** The Constant _PU. */
    private static final String _PU = "oracleNosqlSchemaGeneration";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The property map. */
    private static Map<String, String> propertyMap = new HashMap<String, String>();

    /** The true. */
    private boolean T = true;

    /** The false. */
    private boolean F = false;

    /**
     * Sets the upbefore class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpbeforeClass() throws Exception
    {
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    /**
     * Test crud.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void testCrud() throws Exception
    {
        OracleNoSQLUser user1 = createUser(1, "dev", 23, "dev@abc.com", "noida");
        em.persist(user1);

        em.clear();
        OracleNoSQLUser user = em.find(OracleNoSQLUser.class, 1);
        validateUser1(user);

        user.setName("devender");
        em.merge(user);

        em.clear();
        OracleNoSQLUser u = em.find(OracleNoSQLUser.class, 1);
        Assert.assertNotNull(u);
        Assert.assertEquals("devender", user.getName());

        em.remove(user);

        em.clear();
        u = em.find(OracleNoSQLUser.class, 1);
        Assert.assertNull(u);
    }

    /**
     * Test query.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void testQuery() throws Exception
    {
        OracleNoSQLUser user1 = createUser(1, "dev", 23, "dev@abc.com", "noida");
        OracleNoSQLUser user2 = createUser(2, "karthik", 22, "karthik@abc.com", "indore");
        OracleNoSQLUser user3 = createUser(3, "pg", 24, "pg@abc.com", "blore");

        em.persist(user1);
        em.persist(user2);
        em.persist(user3);

        List<OracleNoSQLUser> users = em.createQuery("select u from OracleNoSQLUser u").getResultList();
        Assert.assertNotNull(users);
        Assert.assertEquals(3, users.size());
        assertResults(users, T, T, T);

        users = em.createQuery("select u from OracleNoSQLUser u where u.userId = 1").getResultList();
        Assert.assertNotNull(users);
        Assert.assertEquals(1, users.size());
        assertResults(users, T, F, F);

        users = em.createQuery("select u from OracleNoSQLUser u where u.name = 'pg'").getResultList();
        Assert.assertNotNull(users);
        Assert.assertEquals(1, users.size());
        assertResults(users, F, F, T);

        users = em.createQuery("select u from OracleNoSQLUser u where u.details.email = 'dev@abc.com'").getResultList();
        Assert.assertNotNull(users);
        Assert.assertEquals(1, users.size());
        assertResults(users, T, F, F);

        em.remove(user1);
        em.remove(user2);
        em.remove(user3);

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
        em.close();
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        emf.close();
    }

    /**
     * Creates the user.
     * 
     * @param id
     *            the id
     * @param name
     *            the name
     * @param age
     *            the age
     * @param email
     *            the email
     * @param address
     *            the address
     * @return the oracle no sql user
     */
    private OracleNoSQLUser createUser(int id, String name, int age, String email, String address)
    {
        OracleNoSQLUser user = new OracleNoSQLUser();
        user.setUserId(id);
        user.setName(name);
        user.setAge(age);
        UserDetails details = new UserDetails();
        details.setEmail(email);
        details.setAddress(address);
        user.setDetails(details);
        return user;
    }

    /**
     * Removes the user.
     */
    private void removeUser()
    {
        OracleNoSQLUser user = em.find(OracleNoSQLUser.class, 1);
        if (user != null)
        {
            em.remove(user);
        }
        user = em.find(OracleNoSQLUser.class, 2);
        if (user != null)
        {
            em.remove(user);
        }
        user = em.find(OracleNoSQLUser.class, 3);
        if (user != null)
        {
            em.remove(user);
        }

    }

    /**
     * Assert results.
     * 
     * @param results
     *            the results
     * @param foundUser1
     *            the found user1
     * @param foundUser2
     *            the found user2
     * @param foundUser3
     *            the found user3
     */
    private void assertResults(List<OracleNoSQLUser> results, boolean foundUser1, boolean foundUser2, boolean foundUser3)
    {
        for (OracleNoSQLUser User : results)
        {
            switch (User.getUserId())
            {
            case 1:
                if (foundUser1)
                    validateUser1(User);
                else
                    Assert.fail();
                break;
            case 2:
                if (foundUser2)
                    validateUser2(User);
                else
                    Assert.fail();
                break;
            case 3:
                if (foundUser3)
                    validateUser3(User);
                else
                    Assert.fail();
                break;
            }
        }
    }

    /**
     * Validate user1.
     * 
     * @param user
     *            the user
     */
    private void validateUser1(OracleNoSQLUser user)
    {
        Assert.assertNotNull(user);
        Assert.assertEquals("dev", user.getName());
        Assert.assertEquals(23, user.getAge());
        Assert.assertEquals("dev@abc.com", user.getDetails().getEmail());
        Assert.assertEquals("noida", user.getDetails().getAddress());
    }

    /**
     * Validate user2.
     * 
     * @param user
     *            the user
     */
    private void validateUser2(OracleNoSQLUser user)
    {
        Assert.assertNotNull(user);
        Assert.assertEquals("karthik", user.getName());
        Assert.assertEquals(22, user.getAge());
        Assert.assertEquals("karthik@abc.com", user.getDetails().getEmail());
        Assert.assertEquals("indore", user.getDetails().getAddress());

    }

    /**
     * Validate user3.
     * 
     * @param user
     *            the user
     */
    private void validateUser3(OracleNoSQLUser user)
    {
        Assert.assertNotNull(user);
        Assert.assertEquals("pg", user.getName());
        Assert.assertEquals(24, user.getAge());
        Assert.assertEquals("pg@abc.com", user.getDetails().getEmail());
        Assert.assertEquals("blore", user.getDetails().getAddress());

    }
}
