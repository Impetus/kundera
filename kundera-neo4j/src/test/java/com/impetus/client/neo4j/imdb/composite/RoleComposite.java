/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.neo4j.imdb.composite;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * Role Relationship entity class
 * @author amresh.singh
 */
@Entity
@Table
@IndexCollection(columns={@Index(name = "roleType", type = "KEYS")})
public class RoleComposite
{
    @EmbeddedId
    private RoleId roleId;
    
    @Column(name="ROLE_TYPE")
    private String roleType;   
    
    @OneToOne
    private ActorComposite actor;
    
    @OneToOne
    private MovieComposite movie;
    
    public RoleComposite() {}
    
    public RoleComposite(RoleId id, String roleType)
    {
        this.roleId = id;
        this.roleType = roleType;
    }   

    /**
     * @return the roleId
     */
    public RoleId getRoleId()
    {
        return roleId;
    }

    /**
     * @param roleId the roleId to set
     */
    public void setRoleId(RoleId roleId)
    {
        this.roleId = roleId;
    }

    /**
     * @return the roleType
     */
    public String getRoleType()
    {
        return roleType;
    }

    /**
     * @param roleType the roleType to set
     */
    public void setRoleType(String roleType)
    {
        this.roleType = roleType;
    }

    /**
     * @return the actor
     */
    public ActorComposite getActor()
    {
        return actor;
    }

    /**
     * @param actor the actor to set
     */
    public void setActor(ActorComposite actor)
    {
        this.actor = actor;
    }

    /**
     * @return the movie
     */
    public MovieComposite getMovie()
    {
        return movie;
    }

    /**
     * @param movie the movie to set
     */
    public void setMovie(MovieComposite movie)
    {
        this.movie = movie;
    } 
    
    
    public boolean equals(Object o)
    {
        if (!(o instanceof RoleComposite))
        {
            return false;
        }

        RoleComposite that = (RoleComposite) o;
        
        return (this.roleId == that.roleId || this.roleId.equals(that.roleId))
                &&(this.roleType == that.roleType || this.roleType.equals(that.roleType));   
        
    }

    public int hashCode()
    {
        int h1 = (roleId == null) ? 0 : roleId.hashCode();
        int h2 = (roleType == null) ? 0 : roleType.hashCode();
        return h1 + 31 * h2;
    }
    
    

}
