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

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.PrePersist;
import javax.persistence.Table;


/**
 * The Class Profile.
 */
@Entity
@Table(name = "Profile", schema = "Blog")
public class Profile
{

    /** The profile id. */
    @Id
    private String profileId;

    /** The address. */
    @Column
    private String address;

    /** The website. */
    @Column
    private String website;

    /** The blog. */
    @Column
    private String blog;

    /** The person. */
    @OneToOne(cascade = { CascadeType.ALL })
    private Person2 person;

    /**
     * Instantiates a new profile.
     */
    public Profile()
    {

    }

    /**
     * Gets the person.
     *
     * @return the person
     */
    public Person2 getPerson()
    {
        return person;
    }

    /**
     * Sets the person.
     *
     * @param person the new person
     */
    public void setPerson(Person2 person)
    {
        this.person = person;
    }

    /**
     * Gets the profile id.
     *
     * @return the profile id
     */
    public String getProfileId()
    {
        return profileId;
    }

    /**
     * Sets the profile id.
     *
     * @param profileId the new profile id
     */
    public void setProfileId(String profileId)
    {
        this.profileId = profileId;
    }

    /**
     * Gets the address.
     *
     * @return the address
     */
    public String getAddress()
    {
        return address;
    }

    /**
     * Sets the address.
     *
     * @param address the new address
     */
    public void setAddress(String address)
    {
        this.address = address;
    }

    /**
     * Gets the website.
     *
     * @return the website
     */
    public String getWebsite()
    {
        return website;
    }

    /**
     * Sets the website.
     *
     * @param website the new website
     */
    public void setWebsite(String website)
    {
        this.website = website;
    }

    /**
     * Gets the blog.
     *
     * @return the blog
     */
    public String getBlog()
    {
        return blog;
    }

    /**
     * Sets the blog.
     *
     * @param blog the new blog
     */
    public void setBlog(String blog)
    {
        this.blog = blog;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Profile [profileId=");
        builder.append(profileId);
        builder.append(", address=");
        builder.append(address);
        builder.append(", blog=");
        builder.append(blog);
        builder.append(", person=");
        // builder.append(person.getUsername());
        builder.append(", website=");
        builder.append(website);
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

}
