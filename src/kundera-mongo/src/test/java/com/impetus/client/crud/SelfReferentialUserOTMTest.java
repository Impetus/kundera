/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Devender Yadav
 * 
 */

public class SelfReferentialUserOTMTest {

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        emf = Persistence.createEntityManagerFactory("mongoTest");
        em = getNewEM();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        em.close();
        emf.close();
    }

    @Test
    public void test() {

        em = getNewEM();

        UserOTM user1 = new UserOTM();
        user1.setUserId("1");
        user1.setUsername("vivek");

        UserOTM user2 = new UserOTM();
        user2.setUserId("2");
        user2.setUsername("dev");

        UserOTM user3 = new UserOTM();
        user3.setUserId("3");
        user3.setUsername("amit");

        user1.addFollower(user2);
        user2.addFollower(user3);

        em.persist(user1);

        em = getNewEM();

        UserOTM u1 = em.find(UserOTM.class, "1");
        Assert.assertEquals("1", u1.getUserId());
        Assert.assertEquals("vivek", u1.getUsername());
        Assert.assertEquals(1, u1.getFollowers().size());
        Assert.assertEquals("2", u1.getFollowers().get(0).getUserId());
        Assert.assertEquals("dev", u1.getFollowers().get(0).getUsername());

        UserOTM u2 = em.find(UserOTM.class, "2");
        Assert.assertEquals("2", u2.getUserId());
        Assert.assertEquals("dev", u2.getUsername());
        Assert.assertEquals(1, u2.getFollowers().size());
        Assert.assertEquals("3", u2.getFollowers().get(0).getUserId());
        Assert.assertEquals("amit", u2.getFollowers().get(0).getUsername());

        UserOTM u3 = em.find(UserOTM.class, "3");
        Assert.assertEquals("3", u3.getUserId());
        Assert.assertEquals("amit", u3.getUsername());
        
        List<UserOTM> userList = em.createQuery("select u from UserOTM u ").getResultList();
        Assert.assertNotNull(userList);
        Assert.assertEquals(3, userList.size());
        
        
        userList = em.createQuery("select u from UserOTM u where u.username = 'vivek'").getResultList();
        Assert.assertEquals(1, userList.size());
        UserOTM user = userList.get(0);
        Assert.assertEquals("1", user.getUserId());
        Assert.assertEquals("vivek", user.getUsername());
        Assert.assertEquals(1, user.getFollowers().size());
        Assert.assertEquals("2", user.getFollowers().get(0).getUserId());
        Assert.assertEquals("dev", user.getFollowers().get(0).getUsername());

        userList = em.createQuery("select u from UserOTM u where u.username = 'dev'").getResultList();
        Assert.assertEquals(1, userList.size());
        user = userList.get(0);
        Assert.assertEquals("2", user.getUserId());
        Assert.assertEquals("dev", user.getUsername());
        Assert.assertEquals(1, user.getFollowers().size());
        Assert.assertEquals("3", user.getFollowers().get(0).getUserId());
        Assert.assertEquals("amit", user.getFollowers().get(0).getUsername());

        userList = em.createQuery("select u from UserOTM u where u.username = 'amit'").getResultList();
        Assert.assertEquals(1, userList.size());
        user = userList.get(0);
        Assert.assertEquals("3", user.getUserId());
        Assert.assertEquals("amit", user.getUsername());

    }

    /**
     * @return
     */
    private EntityManager getNewEM() {
        if (em != null && em.isOpen()) {
            em.close();
        }
        return em = emf.createEntityManager();
    }

}