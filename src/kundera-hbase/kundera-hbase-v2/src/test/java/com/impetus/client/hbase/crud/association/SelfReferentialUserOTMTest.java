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
package com.impetus.client.hbase.crud.association;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * The Class SelfReferentialUserOTMTest.
 * 
 * @author devender.yadav
 */
public class SelfReferentialUserOTMTest
{

    /** The Constant SCHEMA. */
    private static final String SCHEMA = "HBaseNew";

    /** The Constant HBASE_PU. */
    private static final String HBASE_PU = "associationTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(HBASE_PU);
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
     * Test association.
     */
    @Test
    public void testAssociation()
    {

        UserOTM user1 = new UserOTM();
        user1.setUserId("1");
        user1.setUsername("dev");

        UserOTM user2 = new UserOTM();
        user2.setUserId("2");
        user2.setUsername("karthik");

        UserOTM user3 = new UserOTM();
        user3.setUserId("3");
        user3.setUsername("amit");

        user1.addFollower(user2);
        user2.addFollower(user3);

        em.persist(user1);

        UserOTM u1 = em.find(UserOTM.class, "1");
        Assert.assertEquals("1", u1.getUserId());
        Assert.assertEquals("dev", u1.getUsername());
        Assert.assertEquals(1, u1.getFollowers().size());
        Assert.assertEquals("2", u1.getFollowers().get(0).getUserId());
        Assert.assertEquals("karthik", u1.getFollowers().get(0).getUsername());

        UserOTM u2 = em.find(UserOTM.class, "2");
        Assert.assertEquals("2", u2.getUserId());
        Assert.assertEquals("karthik", u2.getUsername());
        Assert.assertEquals(1, u2.getFollowers().size());
        Assert.assertEquals("3", u2.getFollowers().get(0).getUserId());
        Assert.assertEquals("amit", u2.getFollowers().get(0).getUsername());

        UserOTM u3 = em.find(UserOTM.class, "3");
        Assert.assertEquals("3", u3.getUserId());
        Assert.assertEquals("amit", u3.getUsername());

        List<UserOTM> userList = em.createQuery("select u from UserOTM u ").getResultList();
        Assert.assertNotNull(userList);
        Assert.assertEquals(3, userList.size());

        userList = em.createQuery("select u from UserOTM u where u.username = 'dev'").getResultList();
        Assert.assertEquals(1, userList.size());
        UserOTM user = userList.get(0);
        Assert.assertEquals("1", user.getUserId());
        Assert.assertEquals("dev", user.getUsername());
        Assert.assertEquals(1, user.getFollowers().size());
        Assert.assertEquals("2", user.getFollowers().get(0).getUserId());
        Assert.assertEquals("karthik", user.getFollowers().get(0).getUsername());

        userList = em.createQuery("select u from UserOTM u where u.username = 'amit'").getResultList();
        Assert.assertEquals(1, userList.size());
        user = userList.get(0);
        Assert.assertEquals("3", user.getUserId());
        Assert.assertEquals("amit", user.getUsername());
        Assert.assertEquals(0, user.getFollowers().size());
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
        emf = null;
        HBaseTestingUtils.dropSchema(SCHEMA);
    }

}
