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
package com.impetus.client.hbase.crud.inheritence;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * Junit for abstract entity class's operation.
 * 
 * @author Pragalbh Garg
 * 
 */
public class InheritenceDomainTest
{

    /** The pu. */
    protected static String _PU = "inheritenceTest";

    /** The emf. */
    protected static EntityManagerFactory emf;

    /** The em. */
    protected static EntityManager em;

    /** The obj cache. */
    private List objCache = new ArrayList();

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(_PU);
        em = emf.createEntityManager();
    }

    /**
     * Test relation.
     */
    @Test
    public void testRelation()
    {

        List<SocialProfile> profiles = new ArrayList<SocialProfile>();

        FacebookProfile fbprofile = new FacebookProfile();
        fbprofile.setId(103l);
        fbprofile.setFacebookId("fbtestRelation");
        fbprofile.setFacebookUser("facebook");
        fbprofile.setuserType("testRelation");

        profiles.add(fbprofile);

        TwitterProfile twprofile1 = new TwitterProfile();
        twprofile1.setTwitterId("2");
        twprofile1.setTwitterName("testTwitterRelation");
        twprofile1.setId(102l);
        profiles.add(twprofile1);
        twprofile1.setuserType("testRelation");

        UserAccount uacc = new UserAccount();

        uacc.setId(101l);
        uacc.setDispName("Test");
        uacc.setSocialProfiles(profiles);

        twprofile1.setuserAccount(uacc);
        fbprofile.setuserAccount(uacc);

        em.getTransaction().begin();
        em.persist(uacc);
        em.getTransaction().commit();

        objCache.add(uacc);
        objCache.add(twprofile1);
        objCache.add(fbprofile);

        em.clear();

        String uaQuery = "Select ua from UserAccount ua";

        Query q = em.createQuery(uaQuery);
        List<UserAccount> results = q.getResultList();

        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Test", results.get(0).getDispName());
        Assert.assertEquals(2, results.get(0).getSocialProfiles().size());

        em.remove(results.get(0));

        em.clear();

    }

    /**
     * Test abstract entity.
     */
    @Test
    public void testAbstractEntity()
    {
        FacebookProfile fbprofile = new FacebookProfile();
        fbprofile.setId(Long.MIN_VALUE);
        fbprofile.setFacebookId("fbEntity");
        fbprofile.setFacebookUser("facebookEntity");
        fbprofile.setuserType("abstractEntity");

        em.persist(fbprofile);

        TwitterProfile twprofile = new TwitterProfile();
        twprofile.setTwitterId("2");
        twprofile.setTwitterName("twitterEntity");
        twprofile.setId(Long.MAX_VALUE);
        twprofile.setuserType("abstractEntity");

        em.persist(twprofile);

        em.clear();
        objCache.add(twprofile);
        objCache.add(fbprofile);

        SocialProfile facebookProfile = em.find(SocialProfile.class, Long.MIN_VALUE);
        Assert.assertNotNull(facebookProfile);
        Assert.assertTrue(facebookProfile.getClass().isAssignableFrom(FacebookProfile.class));

        SocialProfile twitterProfile = em.find(SocialProfile.class, Long.MAX_VALUE);
        Assert.assertNotNull(twitterProfile);
        Assert.assertTrue(twitterProfile.getClass().isAssignableFrom(TwitterProfile.class));

        em.clear();
        String queryStr = "Select s from SocialProfile s";

        Query query = em.createQuery(queryStr);

        List<SocialProfile> socialProfiles = query.getResultList();

        Assert.assertFalse(socialProfiles.isEmpty());
        Assert.assertEquals(2, socialProfiles.size());
        Assert.assertFalse(socialProfiles.get(0).getClass().getSimpleName()
                .equals(socialProfiles.get(1).getClass().getSimpleName()));

        em.clear();

        String deleteQuery = "Delete from SocialProfile s";
        query = em.createQuery(deleteQuery);
        query.executeUpdate();

        query = em.createQuery(queryStr);

        socialProfiles = query.getResultList();
        Assert.assertTrue(socialProfiles.isEmpty());
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
        for (Object o : objCache)
        {
            em.remove(o);
        }
    }

    /**
     * Tear down after class.
     */
    @AfterClass
    public static void tearDownAfterClass()
    {
        HBaseTestingUtils.dropSchema("HBaseNew");
        if (em != null)
        {
            em.close();
            em = null;
        }
        if (emf != null)
        {
            emf.close();
            emf = null;
        }
    }
}
