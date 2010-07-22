/*
 * Copyright 2010 Impetus Infotech.
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
package com.impetus.kundera.entity;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import com.impetus.kundera.api.SuperColumn;
import com.impetus.kundera.api.SuperColumnFamily;

/**
 * The Class Post.
 * 
 * @author animesh.kumar
 */
@Entity
// makes it an entity class
@SuperColumnFamily(family="Posts", keyspace="Blog")
// assign column-family type and name
public class Post {

    /** The permalink. */
    @Id
    // row identifier
    String permalink;

    /** The title. */
    @Column
    @SuperColumn(column = "post")
    // column 'title' will be stored under super-column 'post'
    String title;

    /** The body. */
    @Column
    @SuperColumn(column = "post")
    String body;

    /** The author. */
    @Column
    @SuperColumn(column = "post")
    String author;

    /** The created. */
    @Column
    @SuperColumn(column = "post")
    Date created;

    /** The tags. */
    @Column
    @SuperColumn(column = "tags")
    // column 'tag' will be stored under super-column 'tags'
    List<String> tags = new ArrayList<String>();

    /**
     * Gets the permalink.
     * 
     * @return the permalink
     */
    public String getPermalink() {
        return permalink;
    }

    /**
     * Sets the permalink.
     * 
     * @param permalink
     *            the permalink to set
     */
    public void setPermalink(String permalink) {
        this.permalink = permalink;
    }

    /**
     * Gets the title.
     * 
     * @return the title
     */
    public String getTitle() {
        return title;
    }

    /**
     * Sets the title.
     * 
     * @param title
     *            the title to set
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Gets the body.
     * 
     * @return the body
     */
    public String getBody() {
        return body;
    }

    /**
     * Sets the body.
     * 
     * @param body
     *            the body to set
     */
    public void setBody(String body) {
        this.body = body;
    }

    /**
     * Gets the author.
     * 
     * @return the author
     */
    public String getAuthor() {
        return author;
    }

    /**
     * Sets the author.
     * 
     * @param author
     *            the author to set
     */
    public void setAuthor(String author) {
        this.author = author;
    }

    /**
     * Gets the created.
     * 
     * @return the created
     */
    public Date getCreated() {
        return created;
    }

    /**
     * Sets the created.
     * 
     * @param created
     *            the created to set
     */
    public void setCreated(Date created) {
        this.created = created;
    }

    /**
     * Gets the tags.
     * 
     * @return the tags
     */
    public List<String> getTags() {
        return tags;
    }

    /**
     * Sets the tags.
     * 
     * @param tags
     *            the tags to set
     */
    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((permalink == null) ? 0 : permalink.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof Post))
            return false;
        Post other = (Post) obj;
        if (permalink == null) {
            if (other.permalink != null)
                return false;
        } else if (!permalink.equals(other.permalink))
            return false;
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Post [title=");
        builder.append(title);
        builder.append(", author=");
        builder.append(author);
        builder.append(", body=");
        builder.append(body);
        builder.append(", created=");
        builder.append(created);
        builder.append(", permalink=");
        builder.append(permalink);
        builder.append(", tags=");
        builder.append(tags);
        builder.append("]");
        return builder.toString();
    }

}
