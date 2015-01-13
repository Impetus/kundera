/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kvapps.runner;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.thrift.ThriftClient;
import com.impetus.kundera.client.Client;
import com.impetus.kvapps.entities.Tweets;
import com.impetus.kvapps.entities.User;
import com.impetus.kvapps.entities.Video;

/**
 * @author impetus
 * 
 * Executor service, responsible for performing various CRUD operation on {@link User} in polyglot way.
 *
 */
public final class ExecutorService {

    /** the log used by this class. */
    private static Log logger = LogFactory.getLog(ExecutorService.class);

    
    /**
     * on query with user email id
     * 
     * @param em         entity manager instance.
     * @param user       user entity instance. 
     * @return           collection of user instance.
     */
    static List<User> onQueryByEmail(final EntityManager em, final User user) 
    {
		// Query by parameter (fetch user by email id)
		String query = "Select u from User u where u.emailId =?1";

		logger.info("");
		logger.info("");
		logger.info(query);
		System.out.println("#######################Querying##########################################");

		// find by named parameter(e.g. email)
		List<User> fetchedUsers = findByEmail(em, query, user.getEmailId());
		return fetchedUsers;
	}

	/**
	 *  On persist user.
	 *  
	 * @param em        entity manager instance.
	 * @param user      user object. 
	 */
	static void onPersist(final EntityManager em, final User user) 
	{
		logger.info("");
		logger.info("");
		logger.info("#######################Persisting##########################################");
		logger.info("");
		logger.info(user.toString());
		persist(user, em);
		logger.info("");
		System.out.println("#######################Persisting##########################################");
		logger.info("");
		logger.info("");
	}

    
    /**
     * On find by user id.
     * 
     * @param em         entity manager instance.
     * @param userId     user id.
     */ 
    static User findByKey(final EntityManager em, final String userId)
    {
        User user = em.find(User.class, userId);
        logger.info("[On Find by key]");
        System.out.println("#######################START##########################################");
        logger.info("\n");
        logger.info("\t\t User's first name:" + user.getFirstName());
        logger.info("\t\t User's emailId:" + user.getEmailId());
        logger.info("\t\t User's Personal Details:" + user.getPersonalDetail().getName());
        logger.info("\t\t User's Personal Details:" + user.getPersonalDetail().getPassword());
        logger.info("\t\t User's total tweets:" + user.getTweets().size());
        logger.info("\n");
        System.out.println("#######################END############################################");
        logger.info("\n");
        return user;
    }

    /**
     *  on find by wild search query.
     *  
     * @param em        entity manager instance.
     * @param query     query. 
     */
    @SuppressWarnings("unchecked")
	static void findByQuery(final EntityManager em, final String query)
    {
        Query q = em.createNamedQuery(query);

        logger.info("[On Find All by Query]");
        List<User> users = q.getResultList();

        if (users == null || users.isEmpty())
        {
            logger.info("0 Users Returned");
            return;
        }

        System.out.println("#######################START##########################################");
        logger.info("\t\t Total number of users:" + users.size());
        logger.info("\t\t User's total tweets:" + users.get(0).getTweets().size());
        printTweets(users);
        logger.info("\n");
        // logger.info("First tweet:" users.get(0).getTweets().);
        System.out.println("#######################END############################################");
        logger.info("\n");
    }

    /**
     * On find by native CQL3 query.
     * 
     * @param em            entity manager instance.
     * @param query         native cql3 query.
     */
    @SuppressWarnings("unchecked")
	static void findByNativeQuery(final EntityManager em, final String query)
    {

        Query q = em.createNativeQuery(query, Tweets.class);

        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        ThriftClient client = (ThriftClient) clients.get("twissandra");
        client.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        logger.info("[On Find Tweets by CQL3]");
        List<Tweets> tweets = q.getResultList();

        System.out.println("#######################START##########################################");
        logger.info("\t\t User's total tweets:" + tweets.size());
        onPrintTweets(tweets);
        logger.info("\n");
        // logger.info("First tweet:" users.get(0).getTweets().);
        System.out.println("#######################END############################################");
        logger.info("\n");
    }

    /**
     *  On print tweets event.
     *  
     * @param tweets
     */
    private static void onPrintTweets(final List<Tweets> tweets)
    {
        for (Iterator<Tweets> iterator = tweets.iterator(); iterator.hasNext();)
        {

            int counter = 1;
            while (iterator.hasNext())
            {
                logger.info("\n");
                logger.info("\t\t Tweet No:#" + counter++);
                Tweets rec = (Tweets) iterator.next();
                logger.info("\t\t tweet is ->" + rec.getBody());
                logger.info("\t\t Tweeted at ->" + rec.getTweetDate());

                if (rec.getVideos() != null)
                {
                    logger.info("\t\t Tweeted Contains Video ->" + rec.getVideos().size());

                    for (Iterator<Video> iteratorVideo = rec.getVideos().iterator(); iteratorVideo.hasNext();)
                    {
                        Video video = (Video) iteratorVideo.next();
                        logger.info(video);
                    }
                }

            }
        }

    }


    private static List<User> findByEmail(final EntityManager em, String query, String parameter)
    {
        Query q = em.createNamedQuery(query);
        q.setParameter(1, parameter);

        List<User> users = q.getResultList();

        return users;
    }

    private static void printTweets(List<User> users)
    {
        for (Iterator<User> iterator = users.iterator(); iterator.hasNext();)
        {
            User user = (User) iterator.next();
            Iterator<Tweets> tweets = users.get(0).getTweets().iterator();

            int counter = 1;
            while (tweets.hasNext())
            {
                logger.info("\n");
                logger.info("\t\t Tweet No:#" + counter++);
                Tweets rec = tweets.next();
                logger.info("\t\t tweet is ->" + rec.getBody());
                logger.info("\t\t Tweeted at ->" + rec.getTweetDate());

                if (rec.getVideos() != null)
                {
                    logger.info("\t\t Tweeted Contains Video ->" + rec.getVideos().size());

                    for (Iterator iteratorVideo = rec.getVideos().iterator(); iteratorVideo.hasNext();)
                    {
                        Video video = (Video) iteratorVideo.next();
                        logger.info(video);
                    }
                }

            }
        }

    }

    /**
     * @param user
     */
    private static void persist(User user, final EntityManager em)
    {
        em.persist(user);
    }

}