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

import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * Movie Node entity class 
 * @author amresh.singh
 */

@Entity
@Table
@IndexCollection(columns={@Index(name = "title", type = "KEYS"), @Index(name = "year", type = "KEYS")})
public class Movie
{
    @Id
    @Column(name="MOVIE_ID")
    private String id;
    
    @Column(name="TITLE")
    private String title;
    
    @Column(name="YEAR")
    private int year;      
    
    @ManyToMany(fetch = FetchType.LAZY, mappedBy="movies")
    private Map<Role, Actor> actors;
    
    public Movie() {}
    public Movie(String id, String title, int year)
    {
        this.id = id;
        this.title = title;
        this.year = year;
    }
    
    public void addActor(Role role, Actor actor)
    {
        if(actors == null) actors = new HashMap<Role, Actor>();
        actors.put(role, actor);
    }

    /**
     * @return the id
     */
    public String getId()
    {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(String id)
    {
        this.id = id;
    }

    /**
     * @return the title
     */
    public String getTitle()
    {
        return title;
    }

    /**
     * @param title the title to set
     */
    public void setTitle(String title)
    {
        this.title = title;
    }

    /**
     * @return the year
     */
    public int getYear()
    {
        return year;
    }

    /**
     * @param year the year to set
     */
    public void setYear(int year)
    {
        this.year = year;
    }

    /**
     * @return the actors
     */
    public Map<Role, Actor> getActors()
    {
        return actors;
    }

    /**
     * @param actors the actors to set
     */
    public void setActors(Map<Role, Actor> actors)
    {
        this.actors = actors;
    }   

}
