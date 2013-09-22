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

import java.util.HashMap;
import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.FetchType;
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
@Table
// Ignored for Neo4J
@IndexCollection(columns = { @Index(name = "name", type = "KEYS") })
public class ActorComposite
{
    @EmbeddedId
    private ActorId actorId;

    @Column(name = "ACTOR_NAME")
    private String name;

    public ActorComposite()
    {
    }

    public ActorComposite(ActorId actorId, String actorName)
    {
        this.actorId = actorId;
        this.name = actorName;
    }

    @ManyToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @MapKeyJoinColumn(name = "ACTS_IN")
    private Map<RoleComposite, MovieComposite> movies;

    public void addMovie(RoleComposite role, MovieComposite movie)
    {
        if (movies == null)
            movies = new HashMap<RoleComposite, MovieComposite>();
        movies.put(role, movie);
    }

    /**
     * @return the actorId
     */
    public ActorId getActorId()
    {
        return actorId;
    }

    /**
     * @param actorId
     *            the actorId to set
     */
    public void setActorId(ActorId actorId)
    {
        this.actorId = actorId;
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
    public Map<RoleComposite, MovieComposite> getMovies()
    {
        return movies;
    }

    /**
     * @param movies
     *            the movies to set
     */
    public void setMovies(Map<RoleComposite, MovieComposite> movies)
    {
        this.movies = movies;
    }

}
