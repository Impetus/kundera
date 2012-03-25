/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.client.entity;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PrePersist;
import javax.persistence.Table;

import com.impetus.kundera.annotations.Index;

/**
 * The Class Person2.
 *
 * @author animesh.kumar
 */
@Entity
@Table(name = "Person", schema = "Blog")
@Index(index = false)
public class Person2 implements Serializable
{

    /** The username. */
    @Id
    private String username;

    /** The password. */
    @Column
    private String password;

    /** The profile. */
    @OneToOne(cascade = { CascadeType.PERSIST, CascadeType.REMOVE })
    private Profile profile;

    /** The public profile. */
    @OneToOne(cascade = { CascadeType.PERSIST, CascadeType.MERGE, CascadeType.REMOVE })
    private Profile publicProfile;

    /** The post. */
    @OneToMany(cascade = { CascadeType.ALL })
    // (targetEntity=Post.class)
    private Set<Post> post = new HashSet<Post>();

    /**
     * Instantiates a new person2.
     */
    public Person2()
    {

    }

    /**
     * Gets the username.
     *
     * @return the username
     */
    public String getUsername()
    {
        return username;
    }

    /**
     * Sets the username.
     *
     * @param username the new username
     */
    public void setUsername(String username)
    {
        this.username = username;
    }

    /**
     * Gets the password.
     *
     * @return the password
     */
    public String getPassword()
    {
        return password;
    }

    /**
     * Sets the password.
     *
     * @param password the new password
     */
    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * Gets the profile.
     *
     * @return the profile
     */
    public Profile getProfile()
    {
        return profile;
    }

    /**
     * Sets the profile.
     *
     * @param profile the new profile
     */
    public void setProfile(Profile profile)
    {
        this.profile = profile;
    }

    /**
     * Gets the public profile.
     *
     * @return the public profile
     */
    public Profile getPublicProfile()
    {
        return publicProfile;
    }

    /**
     * Sets the public profile.
     *
     * @param publicProfile the new public profile
     */
    public void setPublicProfile(Profile publicProfile)
    {
        this.publicProfile = publicProfile;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Person [username=");
        builder.append(username);
        builder.append(", password=");
        builder.append(password);
        builder.append(", post=");
        builder.append(post);
        builder.append(", profile=");
        builder.append(profile);
        builder.append(", publicProfile=");
        builder.append(publicProfile);
        builder.append("]");
        return builder.toString();
    }

    /**
     * Pre.
     */
    @PrePersist
    public void pre()
    {
        System.out.println("PRE PERSIST >> " + this);
    }

    /**
     * Adds the post.
     *
     * @param e the e
     * @return true, if successful
     * @see java.util.Set#add(java.lang.Object)
     */
    public boolean addPost(Post e)
    {
        return post.add(e);
    }

    /**
     * Size.
     *
     * @return the int
     */
    public int size()
    {
        return post.size();
    }
}
