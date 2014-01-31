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
package com.impetus.kundera.client.crud.inheritence;

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

import com.impetus.client.utils.MongoUtils;

/**
 * Junit test case.
 * @author vivek.mishra
 *
 */
public class InheritenceDomainTest
{

    protected static String _PU = "mongoTest";

    /** The emf. */
    protected static EntityManagerFactory emf;

    /** The em. */
    protected static EntityManager em;

    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        
        
        emf = Persistence.createEntityManagerFactory(_PU);
        em = emf.createEntityManager();
    }

    @Test
    public void testRelation()
    {

        List<SocialProfile> profiles = new ArrayList<SocialProfile>();

        FacebookProfile fbprofile = new FacebookProfile();
        fbprofile.setId(103l);
        fbprofile.setFacebookId("fb1");
        fbprofile.setFacebookUser("facebook");
        fbprofile.setuserType("dumbo");

        profiles.add(fbprofile);

        TwitterProfile twprofile1 = new TwitterProfile();
        twprofile1.setTwitterId("2");
        twprofile1.setTwitterName("test2");
        twprofile1.setId(102l);
        profiles.add(twprofile1);
        twprofile1.setuserType("dumbo");
        
        UserAccount uacc = new UserAccount();

        uacc.setId(101l);
        uacc.setDispName("Test");
        uacc.setSocialProfiles(profiles);

        twprofile1.setuserAccount(uacc);
        fbprofile.setuserAccount(uacc);

        em.getTransaction().begin();
        em.persist(uacc);
        em.getTransaction().commit();
        
        //TODOO: Stack over flow error.
//        em.persist(fbprofile);
//        em.persist(twprofile1);
        em.clear();

        String uaQuery = "Select ua from UserAccount ua";

        Query q = em.createQuery(uaQuery);
        List<UserAccount> results = q.getResultList();


        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Test", results.get(0).getDispName());
         Assert.assertEquals(2, results.get(0).getSocialProfiles().size());

        em.clear();

    }

    @Test
    public void testAbstractEntity()
    {
        FacebookProfile fbprofile = new FacebookProfile();
        fbprofile.setId(Long.MIN_VALUE);
        fbprofile.setFacebookId("fb1");
        fbprofile.setFacebookUser("facebook");
        fbprofile.setuserType("dumbo");

        em.persist(fbprofile);
        
        
        TwitterProfile twprofile = new TwitterProfile();
        twprofile.setTwitterId("2");
        twprofile.setTwitterName("test2");
        twprofile.setId(Long.MAX_VALUE);
        twprofile.setuserType("dumbo");
        
        em.persist(twprofile);
        
        SocialProfile facebookProfile = em.find(SocialProfile.class,Long.MIN_VALUE);
        Assert.assertNotNull(facebookProfile);
        Assert.assertTrue(facebookProfile.getClass().isAssignableFrom(FacebookProfile.class));
        
        SocialProfile twitterProfile = em.find(SocialProfile.class,Long.MAX_VALUE);
        Assert.assertNotNull(twitterProfile);
        Assert.assertTrue(twitterProfile.getClass().isAssignableFrom(TwitterProfile.class));
        
        String queryStr = "Select s from SocialProfile s";
        
        Query query = em.createQuery(queryStr);
        
        List<SocialProfile> socialProfiles = query.getResultList();
        
        Assert.assertFalse(socialProfiles.isEmpty());
        Assert.assertEquals(2, socialProfiles.size());
        Assert.assertFalse(socialProfiles.get(0).getClass().getSimpleName().equals(socialProfiles.get(1).getClass().getSimpleName()));
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
        MongoUtils.dropDatabase(emf, _PU);
    }

     @AfterClass
     public static void tearDownAfterClass()
     {
         if(em != null)
         {
             em.close();
             em=null;
         }
         if(emf != null)
         {
             emf.close();
             emf=null;
         }
         
         
     }
}
