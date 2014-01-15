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

import com.impetus.client.twitter.entities.ExternalLinkHBase;
import com.impetus.client.twitter.entities.PreferenceHBase;
import com.impetus.client.twitter.entities.TweetHbase;
import com.impetus.client.twitter.entities.UserHBase;

/**
 * Data access object class for implementation of twitter.
 * 
 * @author amresh.singh
 */
public class TwitterServiceHbase extends SuperDaoHbase implements TwitterHbase
{
    private EntityManager em;

    private EntityManagerFactory emf;

    public TwitterServiceHbase(String persistenceUnitName)
    {
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
            em.clear();
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
    public void addUser(UserHBase user)
    {
        em.persist(user);
    }

    @Override
    public void addUser(String userId, String name, String password, String relationshipStatus)
    {
        UserHBase user = new UserHBase(userId, name, password, relationshipStatus);
        em.persist(user);

    }

    @Override
    public void savePreference(String userId, PreferenceHBase preference)
    {

        UserHBase user = em.find(UserHBase.class, userId);
        user.setPreference(preference);
        em.persist(user);
    }

    @Override
    public void addExternalLink(String userId, String linkId, String linkType, String linkAddress)
    {
        UserHBase user = em.find(UserHBase.class, userId);
        user.addExternalLink(new ExternalLinkHBase(linkId, linkType, linkAddress));

        em.persist(user);
    }

    @Override
    public void addTweet(String userId, String tweetBody, String device)
    {
        UserHBase user = em.find(UserHBase.class, userId);
        user.addTweet(new TweetHbase(tweetBody, device));
        em.persist(user);
    }

    @Override
    public void startFollowing(String userId, String friendUserId)
    {
        UserHBase user = em.find(UserHBase.class, userId);
        UserHBase friend = em.find(UserHBase.class, friendUserId);

        user.addFriend(friend);
        em.persist(user);

        friend.addFollower(user);
        em.persist(friend);
    }

    @Override
    public void addFollower(String userId, String followerUserId)
    {
        UserHBase user = em.find(UserHBase.class, userId);
        UserHBase follower = em.find(UserHBase.class, followerUserId);

        user.addFollower(follower);
        em.persist(user);
    }

    @Override
    public UserHBase findUserById(String userId)
    {
        UserHBase user = em.find(UserHBase.class, userId);
        return user;
    }

    @Override
    public void removeUser(UserHBase user)
    {
        em.remove(user);
    }

    @Override
    public void mergeUser(UserHBase user)
    {
        em.merge(user);
    }

    @Override
    public List<UserHBase> getAllUsers()
    {

        Query q = em.createQuery("select u from UserHBase u");

        List<UserHBase> users = q.getResultList();

        return users;
    }

    @Override
    public List<TweetHbase> getAllTweets(String userId)
    {
        Query q = em.createQuery("select u from UserHBase u where u.userId =:userId");
        q.setParameter("userId", userId);
        List<UserHBase> users = q.getResultList();
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
    public List<UserHBase> getFollowers(String userId)
    {
        Query q = em.createQuery("select u from UserHBase u where u.userId =:userId");
        q.setParameter("userId", userId);
        List<UserHBase> users = q.getResultList();
        if (users == null || users.isEmpty())
        {
            return null;
        }
        return users.get(0).getFollowers();
    }

    @Override
    public List<TweetHbase> findTweetByBody(String tweetBody)
    {
        Query q = em.createQuery("select u.tweet_body from UserHBase u where u.tweet_body like :body");
        q.setParameter("body", tweetBody);
        List<TweetHbase> tweets = q.getResultList();
        return tweets;
    }

    @Override
    public List<TweetHbase> findTweetByDevice(String deviceName)
    {
        Query q = em.createQuery("select u.tweeted_from from UserHBase u where u.tweeted_from like :device");
        q.setParameter("device", deviceName);
        List<TweetHbase> tweets = q.getResultList();
        return tweets;
    }
}
