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

import com.impetus.client.twitter.entities.PreferenceHBase;
import com.impetus.client.twitter.entities.TweetHbase;
import com.impetus.client.twitter.entities.UserHBase;

/**
 * Single window application for Twitter application. Contains methods for
 * performing CRUD operations on users and their tweets.
 */
public interface TwitterHbase
{

    void addUser(UserHBase user);

    /**
     * Registers a new user with Twitter application
     * 
     * @param userId
     *            the user id
     * @param name
     *            the name
     * @param password
     *            the password
     * @param relationshipStatus
     *            the relationship status
     */
    void addUser(String userId, String name, String password, String relationshipStatus);

    /**
     * Save preference for a given user
     * 
     * @param userId
     *            the user id
     * @param preference
     *            the preference
     */
    void savePreference(String userId, PreferenceHBase preference);

    /**
     * Adds an external link for the given user
     * 
     * @param userId
     *            the user id
     * @param linkType
     *            the link type
     * @param linkAddress
     *            the link address
     */
    void addExternalLink(String userId, String linkId, String linkType, String linkAddress);

    /**
     * Adds a new tweet for a user
     * 
     * @param userId
     *            the user id
     * @param tweetBody
     *            the tweet body
     * @param device
     *            the device
     */
    void addTweet(String userId, String tweetBody, String device);

    /**
     * Makes User whose row key is <code>userId</code> follow a user whose row
     * key is <code>friendUserId</code>
     * 
     * @param userId
     *            the user id
     * @param friendUserId
     *            the friend user id
     */
    void startFollowing(String userId, String friendUserId);

    /**
     * Adds the follower whose row key is <code>followerUserId</code> to User
     * whose row key is <code>userId</code>
     * 
     * @param userId
     *            the user id
     * @param followerUserId
     *            the follower user id
     */
    void addFollower(String userId, String followerUserId);

    UserHBase findUserById(String userId);

    void removeUser(UserHBase user);

    void mergeUser(UserHBase user);

    /**
     * Retrieves all tweets for a given user
     * 
     * @param userId
     *            the user id
     * @return the all tweets
     */
    List<UserHBase> getAllUsers();

    List<TweetHbase> getAllTweets(String userId);

    /**
     * Returns a list of followers for a given user.
     * 
     * @param userId
     *            user id
     * @return list of all followers.
     */
    List<UserHBase> getFollowers(String userId);

    /**
     * Find tweet by tweet body.
     * 
     * @param tweetBody
     *            the tweet body
     * @return the list
     */
    List<TweetHbase> findTweetByBody(String tweetBody);

    /**
     * Find tweet by device.
     * 
     * @param deviceName
     *            the device name
     * @return the list
     */
    List<TweetHbase> findTweetByDevice(String deviceName);

    /**
     * Close.
     */
    void close();

    void createEntityManager();

    void closeEntityManager();

}
