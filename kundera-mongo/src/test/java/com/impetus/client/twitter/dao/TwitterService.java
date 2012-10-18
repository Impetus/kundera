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

import com.impetus.client.twitter.entities.ExternalLinkMongo;
import com.impetus.client.twitter.entities.PreferenceMongo;
import com.impetus.client.twitter.entities.Tweet;
import com.impetus.client.twitter.entities.UserMongo;
import com.impetus.client.utils.MongoUtils;

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
                e.printStackTrace();
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
            MongoUtils.dropDatabase(emf, pu);
            emf.close();
        }
    }

    @Override
    public void addUser(UserMongo user)
    {
        em.persist(user);
    }

    @Override
    public void addUser(String userId, String name, String password, String relationshipStatus)
    {
        UserMongo user = new UserMongo(userId, name, password, relationshipStatus);
        em.persist(user);

    }

    @Override
    public void savePreference(String userId, PreferenceMongo preference)
    {

        UserMongo user = em.find(UserMongo.class, userId);
        user.setPreference(preference);
        em.persist(user);
    }

    @Override
    public void addExternalLink(String userId, String linkId, String linkType, String linkAddress)
    {
        UserMongo user = em.find(UserMongo.class, userId);
        user.addExternalLink(new ExternalLinkMongo(linkId, linkType, linkAddress));

        em.persist(user);
    }

    @Override
    public void addTweet(String userId, String tweetBody, String device)
    {
        UserMongo user = em.find(UserMongo.class, userId);
        user.addTweet(new Tweet(tweetBody, device));
        em.persist(user);
    }

    @Override
    public void startFollowing(String userId, String friendUserId)
    {
        UserMongo user = em.find(UserMongo.class, userId);
        UserMongo friend = em.find(UserMongo.class, friendUserId);

        user.addFriend(friend);
        em.persist(user);

        friend.addFollower(user);
        em.persist(friend);
    }

    @Override
    public void addFollower(String userId, String followerUserId)
    {
        UserMongo user = em.find(UserMongo.class, userId);
        UserMongo follower = em.find(UserMongo.class, followerUserId);

        user.addFollower(follower);
        em.persist(user);
    }

    @Override
    public UserMongo findUserById(String userId)
    {
        UserMongo user = em.find(UserMongo.class, userId);
        return user;
    }

    @Override
    public void removeUser(UserMongo user)
    {
        em.remove(user);
    }

    @Override
    public void mergeUser(UserMongo user)
    {
        em.merge(user);
    }

    @Override
    public List<UserMongo> getAllUsers()
    {

        Query q = em.createQuery("select u from UserMongo u");

        List<UserMongo> users = q.getResultList();

        return users;
    }

    @Override
    public List<Tweet> getAllTweets(String userId)
    {
        Query q = em.createQuery("select u from UserMongo u where u.userId =:userId");
        q.setParameter("userId", userId);
        List<UserMongo> users = q.getResultList();
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
    public List<UserMongo> getFollowers(String userId)
    {
        Query q = em.createQuery("select u from UserMongo u where u.userId =:userId");
        q.setParameter("userId", userId);
        List<UserMongo> users = q.getResultList();
        if (users == null || users.isEmpty())
        {
            return null;
        }
        return users.get(0).getFollowers();
    }

    @Override
    public List<Tweet> findTweetByBody(String tweetBody)
    {
        Query q = em.createQuery("select u.tweet_body from UserMongo u where u.tweet_body like :body");
        q.setParameter("body", tweetBody);
        List<Tweet> tweets = q.getResultList();
        return tweets;
    }

    @Override
    public List<Tweet> findTweetByDevice(String deviceName)
    {
        Query q = em.createQuery("select u.tweeted_from from UserMongo u where u.tweeted_from like :device");
        q.setParameter("device", deviceName);
        List<Tweet> tweets = q.getResultList();
        return tweets;
    }
}
