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
@Table
// Ignored for Neo4J
@IndexCollection(columns = { @Index(name = "name", type = "KEYS") })
public class ActorWithMultipleRelation
{
    @Id
    @Column(name = "ACTOR_ID")
    private int id;

    @Column(name = "ACTOR_NAME")
    private String name;

    public ActorWithMultipleRelation()
    {
    }

    public ActorWithMultipleRelation(int actorId, String actorName)
    {
        this.id = actorId;
        this.name = actorName;
    }

    @ManyToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @MapKeyJoinColumn(name = "ACTS_IN")
    private Map<NewRole, LatestMovie> latestActedMovies;

    @ManyToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @MapKeyJoinColumn(name = "ACTS_IN_FAV")
    private Map<OldRole, ArchivedMovie> oldActedMovies;

    public void addLatestMovie(NewRole role, LatestMovie movie)
    {
        if (latestActedMovies == null)
        {
            latestActedMovies = new HashMap<NewRole, LatestMovie>();
        }
        latestActedMovies.put(role, movie);
    }

    public void addArchivedMovie(OldRole role, ArchivedMovie movie)
    {
        if (oldActedMovies == null)
        {
            oldActedMovies = new HashMap<OldRole, ArchivedMovie>();
        }
        oldActedMovies.put(role, movie);
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
     * @return the latestMovies
     */
    public Map<NewRole, LatestMovie> getLatestMovies()
    {
        return latestActedMovies;
    }

    /**
     * @param latestMovies
     *            the actedMovies to set
     */
    public void setLatestMovies(Map<NewRole, LatestMovie> latestMovies)
    {
        this.latestActedMovies = latestMovies;
    }

    /**
     * @return the archivedMovies
     */
    public Map<OldRole, ArchivedMovie> getArchivedMovies()
    {
        return oldActedMovies;
    }

    /**
     * @param archivedMovies
     *            the favouriteMovies to set
     */
    public void setArchivedMovies(Map<OldRole, ArchivedMovie> archivedMovies)
    {
        this.oldActedMovies = archivedMovies;
    }

}
