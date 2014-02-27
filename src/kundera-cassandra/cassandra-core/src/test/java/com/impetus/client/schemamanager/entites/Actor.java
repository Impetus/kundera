/**
 * 
 */
package com.impetus.client.schemamanager.entites;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * @author Kuldeep Mishra
 * 
 */
@Entity
@Table(name = "Actor", schema = "KunderaCoreExmples@CassandraSchemaOperationTest")
public class Actor
{
    @Id
    @Column(name = "actor_id")
    private String actorId;

    // Element collection, will persist co-located
    @ElementCollection
    @CollectionTable(name = "movie")
    private List<Movie> movies;

    // One to many, will be persisted separately
    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FRIEND_ID")
    private List<Actor> friends; // List of users whom I follow

    // One to many, will be persisted separately
    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FOLLOWER_ID")
    private List<Actor> followers; // List of users who are following me

    /**
     * @return the actorId
     */
    public String getActorId()
    {
        return actorId;
    }

    /**
     * @param actorId
     *            the actorId to set
     */
    public void setActorId(String actorId)
    {
        this.actorId = actorId;
    }

    /**
     * @return the tweets
     */
    public List<Movie> getTweets()
    {
        return movies;
    }

    /**
     * @param movies
     *            the tweets to set
     */
    public void addTweet(Movie tweet)
    {
        if (this.movies == null || this.movies.isEmpty())
        {
            this.movies = new ArrayList<Movie>();
        }
        this.movies.add(tweet);
    }

    /**
     * @return the friends
     */
    public List<Actor> getFriends()
    {
        return friends;
    }

    /**
     * @param friends
     *            the friends to set
     */
    public void addFriend(Actor friend)
    {
        if (this.friends == null || this.friends.isEmpty())
        {
            this.friends = new ArrayList<Actor>();
        }
        this.friends.add(friend);
    }

    /**
     * @return the followers
     */
    public List<Actor> getFollowers()
    {
        return followers;
    }

    /**
     * @param followers
     *            the followers to set
     */
    public void addFollower(Actor follower)
    {
        if (this.followers == null || this.followers.isEmpty())
        {
            this.followers = new ArrayList<Actor>();
        }

        this.followers.add(follower);
    }
}
