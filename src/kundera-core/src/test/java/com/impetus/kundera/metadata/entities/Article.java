/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.metadata.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * Entity class for Blog post
 * @author amresh.singh
 */
@Entity
@Table(name="article", schema="KunderaTests@patest")
@IndexCollection(columns = { @Index(name = "body")})
public class Article
{    
    @Id
    @Column(name="post_id")
    private int postId;
    
    //Body of the post
    @Column(name="body")
    private String body;       
    
    //Useful tags specified by author
    @ElementCollection
    @Column(name="tags")
    private Set<String> tags;  
    
    //List of user IDs who liked this blog post
    @ElementCollection
    @Column(name="liked_by")
    private List<Integer> likedBy;   
    
    //User IDs and their respective comments on this blog
    @ElementCollection
    @Column(name="comments")
    private Map<Integer, String> comments; 

    /**
     * @return the postId
     */
    public int getPostId()
    {
        return postId;
    }

    /**
     * @param postId the postId to set
     */
    public void setPostId(int postId)
    {
        this.postId = postId;
    }

    /**
     * @return the body
     */
    public String getBody()
    {
        return body;
    }

    /**
     * @param body the body to set
     */
    public void setBody(String body)
    {
        this.body = body;
    }

    /**
     * @return the tags
     */
    public Set<String> getTags()
    {
        return tags;
    }

    /**
     * @param tags the tags to set
     */
    public void setTags(Set<String> tags)
    {
        this.tags = tags;
    }

    /**
     * @return the likedBy
     */
    public List<Integer> getLikedBy()
    {
        return likedBy;
    }

    /**
     * @param likedBy the likedBy to set
     */
    public void setLikedBy(List<Integer> likedBy)
    {
        this.likedBy = likedBy;
    }

    /**
     * @return the comments
     */
    public Map<Integer, String> getComments()
    {
        return comments;
    }

    /**
     * @param comments the comments to set
     */
    public void setComments(Map<Integer, String> comments)
    {
        this.comments = comments;
    }
    
    public void addTag(String tag)
    {
        if(tags == null)
        {
            tags = new HashSet<String>();
        }
        tags.add(tag);
    }
    
    public void addLikedBy(int likedByUserId)
    {
        if(likedBy == null)
        {
            likedBy = new ArrayList<Integer>();
        }
        likedBy.add(likedByUserId);
    }
    
    public void addComment(int userId, String comment)
    {
        if(comments == null)
        {
            comments = new HashMap<Integer, String>();
        }
        comments.put(userId, comment);
    }
    
}
