package com.impetus.client.twitter.dao;

import java.util.List;

import com.impetus.client.twitter.entities.PreferenceMongo;
import com.impetus.client.twitter.entities.Tweet;
import com.impetus.client.twitter.entities.UserMongo;

/**
 * Single window application for Twitter application. Contains methods for
 * performing CRUD operations on users and their tweets.
 */
public interface Twitter
{
	
	void addUser(UserMongo user);
	
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
    void savePreference(String userId, PreferenceMongo preference);

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

    UserMongo findUserById(String userId);
    
    void removeUser(UserMongo user);
    
    void mergeUser(UserMongo user);
    
    /**
     * Retrieves all tweets for a given user
     * 
     * @param userId
     *            the user id
     * @return the all tweets
     */   
    List<UserMongo> getAllUsers();
    	
    
    
    List<Tweet> getAllTweets(String userId);

    /**
     * Returns a list of followers for a given user.
     * 
     * @param userId
     *            user id
     * @return list of all followers.
     */
    List<UserMongo> getFollowers(String userId);

    /**
     * Find tweet by tweet body.
     * 
     * @param tweetBody
     *            the tweet body
     * @return the list
     */
    List<Tweet> findTweetByBody(String tweetBody);

    /**
     * Find tweet by device.
     * 
     * @param deviceName
     *            the device name
     * @return the list
     */
    List<Tweet> findTweetByDevice(String deviceName);

    /**
     * Close.
     */
    void close();
    
    void createEntityManager();
    
    void closeEntityManager(); 
    

}
