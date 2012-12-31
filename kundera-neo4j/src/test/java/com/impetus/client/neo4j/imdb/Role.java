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
package com.impetus.client.neo4j.imdb;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Role Relationship entity class
 * @author amresh.singh
 */
@Entity
@Table
public class Role
{
    @Id
    @Column(name="ROLE_NAME")
    private String roleName;
    
    @Column(name="ROLE_TYPE")
    private String roleType;   
    
    private Actor actor;
    
    private Movie movie;
    
    public Role() {}
    
    public Role(String roleName, String roleType)
    {
        this.roleName = roleName;
        this.roleType = roleType;
    }

    /**
     * @return the roleName
     */
    public String getRoleName()
    {
        return roleName;
    }

    /**
     * @param roleName the roleName to set
     */
    public void setRoleName(String roleName)
    {
        this.roleName = roleName;
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
    public Actor getActor()
    {
        return actor;
    }

    /**
     * @param actor the actor to set
     */
    public void setActor(Actor actor)
    {
        this.actor = actor;
    }

    /**
     * @return the movie
     */
    public Movie getMovie()
    {
        return movie;
    }

    /**
     * @param movie the movie to set
     */
    public void setMovie(Movie movie)
    {
        this.movie = movie;
    }  

}
