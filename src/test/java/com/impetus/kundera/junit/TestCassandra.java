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

import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.log4j.Logger;

import com.impetus.kundera.entity.Email;
import com.impetus.kundera.entity.PersonalDetail;
import com.impetus.kundera.entity.Tweet;
import com.impetus.kundera.entity.User;
import com.impetus.kundera.loader.Configuration;

/**
 * Test case for CRUD operations on Cassandra database using Kundera.
 * 
 * @author animesh.kumar
 */
public class TestCassandra extends BaseTest
{

    /** The manager. */
    private EntityManager manager;

    Configuration conf;
    
    private static Logger logger =  Logger.getLogger(TestCassandra.class);
     

    /** The embedded server cassandra. */
    private static EmbeddedCassandraService cassandra;

    public void startCassandraServer() throws Exception
    {
        //super.startCassandraServer();
    }

    /**
     * Sets the up.
     * 
     * @throws java.lang.Exception
     *             * @throws Exception the exception
     * @throws Exception
     *             the exception
     */
    public void setUp() throws Exception
    {
        logger.info("starting server");
        if (cassandra == null)
        {
            startCassandraServer();
        }
        if (conf == null)
        {
            conf = new Configuration();
            manager = conf.getEntityManager("cassandra");
        }

    }

    /**
     * Tear down.
     * 
     * @throws java.lang.Exception
     *             * @throws Exception the exception
     * @throws Exception
     *             the exception
     */
    public void tearDown() throws Exception
    {
        logger.info("destroying conf");
        conf.destroy();
    }
    
   public void testInsertUser() {
    	User user = new User();
    	user.setUserId("IIIPL-0001");   	
    	
    	PersonalDetail pd = new PersonalDetail();
    	pd.setPersonalDetailId("1");
    	pd.setName("Amresh");
    	pd.setPassword("password1");
    	pd.setRelationshipStatus("single");
    	user.setPersonalDetail(pd);
    	
    	user.addTweet(new Tweet("a", "Here it goes, my first tweet", "web"));
    	user.addTweet(new Tweet("b", "Another one from me", "mobile"));
    	
    	manager.persist(user);
    }
    
   /*public void testUpdateUser() {
    	User user = manager.find(User.class, "IIIPL-0001");	
    	
    	PersonalDetail pd = new PersonalDetail();   	
    	
    	pd.setPassword("password2");
    	pd.setRelationshipStatus("married");
    	
    	List<Tweet> tweets = user.getTweets();
    	
    	for(Tweet tweet : tweets) {
    		if(tweet.getTweetId().equals("a")) {
    			tweet.setBody("My first tweet is now modified");
    		} else if(tweet.getTweetId().equals("b")) {
    			tweet.setBody("My second tweet is now modified");
    		}
    	}   	   
    	
    	tweets.add(new Tweet("c", "My Third tweet", "iPhone"));
    	
    	manager.persist(user);
    }*/
    
  /* public void testFindUser() {
    	User user = manager.find(User.class, "IIIPL-0001");
    	System.out.println(user.getUserId() + "(Personal Data): " + user.getPersonalDetail().getName() + "/Tweets:" + user.getTweets());
    }*/
    
    
   /* public void testDeleteUser() {
    	User user = manager.find(User.class, "IIIPL-0001");
    	System.out.println(user);
    	manager.remove(user);
    }*/
   
   /*public void testQuery() {
	   Query q = manager.createQuery("select u from User u");		
		//q.setParameter("subject", "Join");
		//q.setParameter("body", "Please Join Meeting");		
		List<User> users = q.getResultList();		
		System.out.println("Users:" + users);
   }*/

}
