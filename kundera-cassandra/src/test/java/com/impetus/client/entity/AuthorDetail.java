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

import javax.persistence.Column;
import javax.persistence.Embeddable;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * The Class AuthorDetail.
 */
@Embeddable
public class AuthorDetail
{
    /** The author. */
    @Column(name = "authorname")
    public String name;

    /** The email. */
    @Column(name = "email")
    public String email;

    /**
     * Instantiates a new author detail.
     */
    public AuthorDetail()
    {
    }

    /**
     * Gets the author.
     *
     * @return the author
     */
    public String getAuthor()
    {
        return name;
    }

    /**
     * Sets the author.
     *
     * @param author the author to set
     */
    public void setAuthor(String author)
    {
        this.name = author;
    }

    /**
     * Gets the email.
     *
     * @return the email
     */
    public String getEmail()
    {
        return email;
    }

    /**
     * Sets the email.
     *
     * @param email the email to set
     */
    public void setEmail(String email)
    {
        this.email = email;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(", [author=");
        builder.append(name);
        builder.append(", email=");
        builder.append(email);
        builder.append("]");
        return builder.toString();
    }
}
