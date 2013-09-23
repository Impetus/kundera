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
package com.impetus.kundera.tests.crossdatastore.imdb.entities;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.MapKeyJoinColumn;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * Actor Node Entity class
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "ACTOR")
// Ignored for Neo4J
@IndexCollection(columns = { @Index(name = "name", type = "KEYS") })
public class Actor
{
    @Id
    @Column(name = "ACTOR_ID")
    private int id;

    @Column(name = "ACTOR_NAME")
    private String name;

    public Actor()
    {
    }

    public Actor(int actorId, String actorName)
    {
        this.id = actorId;
        this.name = actorName;
    }

    @ManyToMany(cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @MapKeyJoinColumn(name = "ACTS_IN")
    private Map<Role, Movie> movies;

    public void addMovie(Role role, Movie movie)
    {
        if (movies == null)
            movies = new HashMap<Role, Movie>();
        movies.put(role, movie);
    }

    /**
     * @return the id
     */
    public int getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(int id)
    {
        this.id = id;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * @return the movies
     */
    public Map<Role, Movie> getMovies()
    {
        return movies;
    }

    /**
     * @param movies
     *            the movies to set
     */
    public void setMovies(Map<Role, Movie> movies)
    {
        this.movies = movies;
    }

}
