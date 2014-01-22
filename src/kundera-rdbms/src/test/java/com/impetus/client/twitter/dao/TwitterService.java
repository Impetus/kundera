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
package com.impetus.client.twitter.dao;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import com.impetus.client.twitter.entities.ExternalLinkRDBMS;
import com.impetus.client.twitter.entities.PreferenceRDBMS;
import com.impetus.client.twitter.entities.TweetRDBMS;
import com.impetus.client.twitter.entities.UserRDBMS;

/**
 * Data access object class for implementation of twitter.
 * 
 * @author amresh.singh
 */
public class TwitterService extends SuperDao implements Twitter
{
    private EntityManager em;

    private EntityManagerFactory emf;

    private String pu;

    public TwitterService(String persistenceUnitName)
    {
        this.pu = persistenceUnitName;
        if (emf == null)
        {
            try
            {
                emf = createEntityManagerFactory(persistenceUnitName);
            }
            catch (Exception e)
            {
                
            }
        }

    }

    @Override
    public void createEntityManager()
    {
        if (em == null)
        {
            em = emf.createEntityManager();
        }
    }

    @Override
    public void closeEntityManager()
    {
        if (em != null)
        {
            em.close();
            em = null;
        }
    }

    @Override
    public void close()
    {
        if (emf != null)
        {
            emf.close();
        }
    }

    @Override
    public void addUser(UserRDBMS user)
    {
        em.persist(user);
    }

    @Override
    public void addUser(String userId, String name, String password, String relationshipStatus)
    {
        UserRDBMS user = new UserRDBMS(userId, name, password, relationshipStatus);
        em.persist(user);

    }

    @Override
    public void savePreference(String userId, PreferenceRDBMS preference)
    {

        UserRDBMS user = em.find(UserRDBMS.class, userId);
        user.setPreference(preference);
        em.persist(user);
    }

    @Override
    public void addExternalLink(String userId, String linkId, String linkType, String linkAddress)
    {
        UserRDBMS user = em.find(UserRDBMS.class, userId);
        user.addExternalLink(new ExternalLinkRDBMS(linkId, linkType, linkAddress));

        em.persist(user);
    }

    @Override
    public void addTweet(String userId, String tweetBody, String device)
    {
        UserRDBMS user = em.find(UserRDBMS.class, userId);
        user.addTweet(new TweetRDBMS(tweetBody, device));
        em.persist(user);
    }

    @Override
    public void startFollowing(String userId, String friendUserId)
    {
        UserRDBMS user = em.find(UserRDBMS.class, userId);
        UserRDBMS friend = em.find(UserRDBMS.class, friendUserId);

        user.addFriend(friend);
        em.persist(user);

        friend.addFollower(user);
        em.persist(friend);
    }

    @Override
    public void addFollower(String userId, String followerUserId)
    {
        UserRDBMS user = em.find(UserRDBMS.class, userId);
        UserRDBMS follower = em.find(UserRDBMS.class, followerUserId);

        user.addFollower(follower);
        em.persist(user);
    }

    @Override
    public UserRDBMS findUserById(String userId)
    {
        UserRDBMS user = em.find(UserRDBMS.class, userId);
        return user;
    }

    @Override
    public void removeUser(UserRDBMS user)
    {
        em.remove(user);
    }

    @Override
    public void mergeUser(UserRDBMS user)
    {
        em.merge(user);
    }

    @Override
    public List<UserRDBMS> getAllUsers()
    {

        Query q = em.createQuery("select u from UserRDBMS u");

        List<UserRDBMS> users = q.getResultList();

        return users;
    }

    @Override
    public List<TweetRDBMS> getAllTweets(String userId)
    {
        Query q = em.createQuery("select u from UserRDBMS u where u.userId =:userId");
        q.setParameter("userId", userId);
        List<UserRDBMS> users = q.getResultList();
        if (users == null || users.isEmpty())
        {
            return null;
        }
        else
        {
            return users.get(0).getTweets();
        }
    }

    @Override
    public List<UserRDBMS> getFollowers(String userId)
    {
        Query q = em.createQuery("select u from UserRDBMS u where u.userId =:userId");
        q.setParameter("userId", userId);
        List<UserRDBMS> users = q.getResultList();
        if (users == null || users.isEmpty())
        {
            return null;
        }
        return users.get(0).getFollowers();
    }

    @Override
    public List<TweetRDBMS> findTweetByBody(String tweetBody)
    {
        Query q = em.createQuery("select u.tweet_body from UserRDBMS u where u.tweet_body like :body");
        q.setParameter("body", tweetBody);
        List<TweetRDBMS> tweets = q.getResultList();
        return tweets;
    }

    @Override
    public List<TweetRDBMS> findTweetByDevice(String deviceName)
    {
        Query q = em.createQuery("select u.tweeted_from from UserRDBMS u where u.tweeted_from like :device");
        q.setParameter("device", deviceName);
        List<TweetRDBMS> tweets = q.getResultList();
        return tweets;
    }
}
