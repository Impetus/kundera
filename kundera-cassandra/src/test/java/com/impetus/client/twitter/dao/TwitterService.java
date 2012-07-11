/*
 * Copyright 2011 Impetus Infotech.
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
package com.impetus.client.twitter.dao;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import com.impetus.client.twitter.entities.ExternalLinkCassandra;
import com.impetus.client.twitter.entities.PreferenceCassandra;
import com.impetus.client.twitter.entities.Tweet;
import com.impetus.client.twitter.entities.UserCassandra;

/**
 * Data access object class for implementation of twitter.
 * 
 * @author amresh.singh
 */
public class TwitterService extends SuperDao implements Twitter
{
    private EntityManager em;

    private EntityManagerFactory emf;

    public TwitterService(String persistenceUnitName)
    {
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
            emf.close();
        }
    }

    @Override
    public void addUser(UserCassandra user)
    {
        em.persist(user);
    }

    @Override
    public void addUser(String userId, String name, String password, String relationshipStatus)
    {
        UserCassandra user = new UserCassandra(userId, name, password, relationshipStatus);
        em.persist(user);

    }

    @Override
    public void savePreference(String userId, PreferenceCassandra preference)
    {

        UserCassandra user = em.find(UserCassandra.class, userId);
        user.setPreference(preference);
        em.persist(user);
    }

    @Override
    public void addExternalLink(String userId, String linkId, String linkType, String linkAddress)
    {
        UserCassandra user = em.find(UserCassandra.class, userId);
        user.addExternalLink(new ExternalLinkCassandra(linkId, linkType, linkAddress));

        em.persist(user);
    }

    @Override
    public void addTweet(String userId, String tweetBody, String device)
    {
        UserCassandra user = em.find(UserCassandra.class, userId);
        user.addTweet(new Tweet(tweetBody, device));
        em.persist(user);
    }

    @Override
    public void startFollowing(String userId, String friendUserId)
    {
        UserCassandra user = em.find(UserCassandra.class, userId);
        UserCassandra friend = em.find(UserCassandra.class, friendUserId);

        user.addFriend(friend);
        em.persist(user);

        friend.addFollower(user);
        em.persist(friend);
    }

    @Override
    public void addFollower(String userId, String followerUserId)
    {
        UserCassandra user = em.find(UserCassandra.class, userId);
        UserCassandra follower = em.find(UserCassandra.class, followerUserId);

        user.addFollower(follower);
        em.persist(user);
    }

    @Override
    public UserCassandra findUserById(String userId)
    {
        UserCassandra user = em.find(UserCassandra.class, userId);
        return user;
    }

    @Override
    public void removeUser(UserCassandra user)
    {
        em.remove(user);
    }

    @Override
    public void mergeUser(UserCassandra user)
    {
        em.merge(user);
    }

    @Override
    public List<UserCassandra> getAllUsers()
    {

        Query q = em.createQuery("select u from UserCassandra u");

        List<UserCassandra> users = q.getResultList();

        return users;
    }

    @Override
    public List<Tweet> getAllTweets(String userId)
    {
        Query q = em.createQuery("select u from UserCassandra u where u.userId =:userId");
        q.setParameter("userId", userId);
        List<UserCassandra> users = q.getResultList();
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
    public List<UserCassandra> getFollowers(String userId)
    {
        Query q = em.createQuery("select u from UserCassandra u where u.userId =:userId");
        q.setParameter("userId", userId);
        List<UserCassandra> users = q.getResultList();
        if (users == null || users.isEmpty())
        {
            return null;
        }
        return users.get(0).getFollowers();
    }
    
    @Override
    public List<UserCassandra> findPersonalDetailByName(String name)
    {
        Query q = em.createQuery("select u.personalDetail.name from UserCassandra u where u.personalDetail.name =:name");
        q.setParameter("name", name);
        List<UserCassandra> users = q.getResultList();        
        return users;        
    }

    @Override
    public List<Tweet> findTweetByBody(String tweetBody)
    {
        Query q = em.createQuery("select u.tweets.body from UserCassandra u where u.tweets.body like :body");
        q.setParameter("body", tweetBody);
        List<Tweet> tweets = q.getResultList();
        return tweets;
    }

    @Override
    public List<Tweet> findTweetByDevice(String deviceName)
    {
        Query q = em.createQuery("select u.tweets.device from UserCassandra u where u.tweets.device like :device");
        q.setParameter("device", deviceName);
        List<Tweet> tweets = q.getResultList();
        return tweets;
    }
}
