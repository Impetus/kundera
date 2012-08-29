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
package com.impetus.client.twitter;

import java.util.List;

import junit.framework.TestCase;

import org.junit.Assert;

import com.impetus.client.hbase.crud.LuceneCleanupUtilities;
import com.impetus.client.twitter.dao.Twitter;
import com.impetus.client.twitter.dao.TwitterService;
import com.impetus.client.twitter.entities.ExternalLinkHBase;
import com.impetus.client.twitter.entities.PersonalDetail;
import com.impetus.client.twitter.entities.PreferenceHBase;
import com.impetus.client.twitter.entities.Tweet;
import com.impetus.client.twitter.entities.UserHBase;

/**
 * Test case for MongoDB.
 * 
 * @author amresh.singh
 */
public abstract class TwitterTestBase extends TestCase
{
    public static final boolean RUN_IN_EMBEDDED_MODE = false;

    public static final boolean AUTO_MANAGE_SCHEMA = false;

    /** The user id1. */
    String userId1;

    /** The user id2. */
    String userId2;

    /** The twitter. */
    protected Twitter twitter;

    private String persistenceUnitName;

    /**
     * Sets the up internal.
     * 
     * @param persistenceUnitName
     *            the new up internal
     * @throws Exception
     *             the exception
     */
    protected void setUpInternal(String persistenceUnitName) throws Exception
    {
        this.persistenceUnitName = persistenceUnitName;
        userId1 = "0001";
        userId2 = "0002";

        // Start Cassandra Server
        if (RUN_IN_EMBEDDED_MODE)
        {
            startServer();
        }

        twitter = new TwitterService(persistenceUnitName);

        // Create Schema
        if (AUTO_MANAGE_SCHEMA)
        {
            createSchema();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#tearDown()
     */
    /**
     * Tear down internal.
     * 
     * @throws Exception
     *             the exception
     */
    protected void tearDownInternal() throws Exception
    {
        if (twitter != null)
        {
            twitter.close();
        }

        if (AUTO_MANAGE_SCHEMA)
        {
            deleteSchema();
        }

        // Stop Server
        if (RUN_IN_EMBEDDED_MODE)
        {
            stopServer();
        }

        LuceneCleanupUtilities.cleanLuceneDirectory(persistenceUnitName);
    }

    /**
     * Execute suite.
     */
    protected void executeTestSuite()
    {
        // Insert, Find and Update
        addAllUserInfo();
        getUserById();
        updateUser();

        // Queries
        getAllUsers();
        getAllTweets();

        // Remove Users
        removeUser();

    }

    protected void addAllUserInfo()
    {
        UserHBase user1 = buildUser1();
        UserHBase user2 = buildUser2();

        twitter.createEntityManager();
        twitter.addUser(user1);
        twitter.closeEntityManager();

        twitter.createEntityManager();
        twitter.addUser(user2);
        twitter.closeEntityManager();
    }

    protected void getUserById()
    {
        twitter.createEntityManager();
        UserHBase user1 = twitter.findUserById(userId1);
        assertUser1(user1);

        UserHBase user2 = twitter.findUserById(userId2);
        assertUser2(user2);

    }

    protected void updateUser()
    {
        twitter.createEntityManager();
        UserHBase user1 = twitter.findUserById(userId1);
        assertUser1(user1);

        user1.setPersonalDetail(new PersonalDetail("Vivek", "unknown", "Married"));
        user1.addTweet(new Tweet("My Third Tweet", "iPhone"));
        twitter.mergeUser(user1);

        UserHBase user1AfterMerge = twitter.findUserById(userId1);

        assertUpdatedUser1(user1AfterMerge);

        twitter.closeEntityManager();
    }

    protected void removeUser()
    {
        twitter.createEntityManager();
        UserHBase user1 = twitter.findUserById(userId1);
        assertUpdatedUser1(user1);

        twitter.removeUser(user1);

        UserHBase user1AfterRemoval = twitter.findUserById(userId1);
        Assert.assertNull(user1AfterRemoval);

        twitter.closeEntityManager();

    }

    protected void getAllUsers()
    {
        twitter.createEntityManager();
        List<UserHBase> users = twitter.getAllUsers();
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertEquals(2, users.size());

        for (UserHBase u : users)
        {
            Assert.assertNotNull(u);
            if (u.getUserId().equals(userId1))
            {
                assertUpdatedUser1(u);
            }
            else if (u.getUserId().equals(userId2))
            {
                assertUser2(u);
            }
        }
        twitter.closeEntityManager();
    }

    /**
     * Adds the users.
     */
    protected void addUsers()
    {
        twitter.createEntityManager();
        twitter.addUser(userId1, "Amresh", "password1", "married");
        twitter.closeEntityManager();

        twitter.createEntityManager();
        twitter.addUser(userId2, "Saurabh", "password2", "single");
        twitter.closeEntityManager();
    }

    /**
     * Save preference.
     */
    protected void savePreference()
    {
        twitter.createEntityManager();
        twitter.savePreference(userId1, new PreferenceHBase("P1", "Motif", "2"));
        twitter.closeEntityManager();

        twitter.createEntityManager();
        twitter.savePreference(userId2, new PreferenceHBase("P2", "High Contrast", "3"));
        twitter.closeEntityManager();
    }

    /**
     * Adds the external links.
     */
    protected void addExternalLinks()
    {
        twitter.createEntityManager();
        twitter.addExternalLink(userId1, "L1", "Facebook", "http://facebook.com/coolnerd");
        twitter.closeEntityManager();

        twitter.createEntityManager();
        twitter.addExternalLink(userId1, "L2", "LinkedIn", "http://linkedin.com/in/devilmate");
        twitter.closeEntityManager();

        twitter.createEntityManager();
        twitter.addExternalLink(userId2, "L3", "GooglePlus", "http://plus.google.com/inviteme");
        twitter.closeEntityManager();

        twitter.createEntityManager();
        twitter.addExternalLink(userId2, "L4", "Yahoo", "http://yahoo.com/profiles/itsmeamry");
        twitter.closeEntityManager();
    }

    /**
     * Adds the tweets.
     */
    protected void addTweets()
    {
        twitter.createEntityManager();
        twitter.addTweet(userId1, "Here is my first tweet", "Web");
        twitter.closeEntityManager();

        twitter.createEntityManager();
        twitter.addTweet(userId1, "Second Tweet from me", "Mobile");
        twitter.closeEntityManager();

        twitter.createEntityManager();
        twitter.addTweet(userId2, "Saurabh tweets for the first time", "Phone");
        twitter.closeEntityManager();

        twitter.createEntityManager();
        twitter.addTweet(userId2, "Another tweet from Saurabh", "text");
        twitter.closeEntityManager();
    }

    /**
     * User1 follows user2.
     */
    protected void user1FollowsUser2()
    {
        twitter.createEntityManager();
        twitter.startFollowing(userId1, userId2);
        twitter.closeEntityManager();
    }

    /**
     * User1 adds user2 as follower.
     */
    protected void user1AddsUser2AsFollower()
    {
        twitter.createEntityManager();
        twitter.addFollower(userId1, userId2);
        twitter.closeEntityManager();
    }

    /**
     * Gets the all tweets.
     * 
     * @return the all tweets
     */
    protected void getAllTweets()
    {
        twitter.createEntityManager();

        List<Tweet> tweetsUser1 = twitter.getAllTweets(userId1);
        List<Tweet> tweetsUser2 = twitter.getAllTweets(userId2);

        twitter.closeEntityManager();

        assertNotNull(tweetsUser1);
        assertNotNull(tweetsUser2);

        assertFalse(tweetsUser1.isEmpty());
        assertFalse(tweetsUser2.isEmpty());

        assertEquals(3, tweetsUser1.size());
        assertEquals(2, tweetsUser2.size());
    }

    /**
     * Gets the tweets by body.
     * 
     * @return the tweets by body
     */
    public void getTweetsByBody()
    {
        twitter.createEntityManager();
        List<Tweet> user1Tweet = twitter.findTweetByBody("Here");
        List<Tweet> user2Tweet = twitter.findTweetByBody("Saurabh");

        twitter.closeEntityManager();

        assertNotNull(user1Tweet);
        assertNotNull(user2Tweet);
        assertEquals(1, user1Tweet.size());
        assertEquals(1, user2Tweet.size());
    }

    /**
     * Gets the tweet by device.
     * 
     * @return the tweet by device
     */
    public void getTweetsByDevice()
    {
        twitter.createEntityManager();
        List<Tweet> webTweets = twitter.findTweetByDevice("Web");
        List<Tweet> mobileTweets = twitter.findTweetByDevice("Mobile");

        twitter.closeEntityManager();

        assertNotNull(webTweets);
        assertNotNull(mobileTweets);
        assertEquals(1, webTweets.size());
        assertEquals(1, mobileTweets.size());

    }

    /**
     * Gets the all followers.
     * 
     * @return the all followers
     */
    protected void getAllFollowers()
    {
        twitter.createEntityManager();
        List<UserHBase> follower1 = twitter.getFollowers(userId1);
        twitter.closeEntityManager();

        twitter.createEntityManager();
        List<UserHBase> follower2 = twitter.getFollowers(userId2);
        twitter.closeEntityManager();

        assertNull(follower1);
        assertNotNull(follower2);
    }

    /**
     * @return
     */
    private UserHBase buildUser1()
    {
        UserHBase user1 = new UserHBase(userId1, "Amresh", "password1", "married");

        user1.setPreference(new PreferenceHBase("P1", "Motif", "2"));

        user1.addExternalLink(new ExternalLinkHBase("L1", "Facebook", "http://facebook.com/coolnerd"));
        user1.addExternalLink(new ExternalLinkHBase("L2", "LinkedIn", "http://linkedin.com/in/devilmate"));

        user1.addTweet(new Tweet("Here is my first tweet", "Web"));
        user1.addTweet(new Tweet("Second Tweet from me", "Mobile"));
        return user1;
    }

    /**
     * @return
     */
    private UserHBase buildUser2()
    {
        UserHBase user2 = new UserHBase(userId2, "Saurabh", "password2", "single");

        user2.setPreference(new PreferenceHBase("P2", "High Contrast", "3"));

        user2.addExternalLink(new ExternalLinkHBase("L3", "GooglePlus", "http://plus.google.com/inviteme"));
        user2.addExternalLink(new ExternalLinkHBase("L4", "Yahoo", "http://yahoo.com/profiles/itsmeamry"));

        user2.addTweet(new Tweet("Saurabh tweets for the first time", "Phone"));
        user2.addTweet(new Tweet("Another tweet from Saurabh", "text"));
        return user2;
    }

    private void assertUser1(UserHBase user1)
    {
        Assert.assertNotNull(user1);
        Assert.assertEquals(userId1, user1.getUserId());
        Assert.assertNotNull(user1.getPersonalDetail());
        Assert.assertEquals("Amresh", user1.getPersonalDetail().getName());
        Assert.assertNotNull(user1.getPreference());
        Assert.assertEquals("2", user1.getPreference().getPrivacyLevel());
        Assert.assertNotNull(user1.getTweets());
        Assert.assertFalse(user1.getTweets().isEmpty());
        Assert.assertEquals(2, user1.getTweets().size());
        Assert.assertNotNull(user1.getExternalLinks());
        Assert.assertFalse(user1.getExternalLinks().isEmpty());
        Assert.assertEquals(2, user1.getExternalLinks().size());
    }

    private void assertUser2(UserHBase user2)
    {
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        Assert.assertNotNull(user2.getPersonalDetail());
        Assert.assertEquals("Saurabh", user2.getPersonalDetail().getName());
        Assert.assertNotNull(user2.getPreference());
        Assert.assertEquals("3", user2.getPreference().getPrivacyLevel());
        Assert.assertNotNull(user2.getTweets());
        Assert.assertFalse(user2.getTweets().isEmpty());
        Assert.assertEquals(2, user2.getTweets().size());
        Assert.assertNotNull(user2.getExternalLinks());
        Assert.assertFalse(user2.getExternalLinks().isEmpty());
        Assert.assertEquals(2, user2.getExternalLinks().size());
    }

    private void assertUpdatedUser1(UserHBase user1)
    {
        Assert.assertNotNull(user1);
        Assert.assertEquals(userId1, user1.getUserId());
        Assert.assertNotNull(user1.getPersonalDetail());
        Assert.assertEquals("Vivek", user1.getPersonalDetail().getName());
        Assert.assertEquals("unknown", user1.getPersonalDetail().getPassword());
        Assert.assertNotNull(user1.getPreference());
        Assert.assertEquals("2", user1.getPreference().getPrivacyLevel());
        Assert.assertNotNull(user1.getTweets());
        Assert.assertFalse(user1.getTweets().isEmpty());
        Assert.assertEquals(3, user1.getTweets().size());
        Assert.assertNotNull(user1.getExternalLinks());
        Assert.assertFalse(user1.getExternalLinks().isEmpty());
        Assert.assertEquals(2, user1.getExternalLinks().size());
    }

    abstract void startServer();

    abstract void stopServer();

    abstract void deleteSchema();

    abstract void createSchema();
}
