/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.tests.crossdatastore.imdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.tests.cli.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.imdb.entities.Actor;
import com.impetus.kundera.tests.crossdatastore.imdb.entities.Movie;
import com.impetus.kundera.tests.crossdatastore.imdb.entities.Role;

public class IMDBPolyglotTest extends TwinAssociation

{  
    Set<Actor> actors = new HashSet<Actor>(); 

    @BeforeClass
    public static void init() throws Exception
    {

        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(Actor.class);
        clazzz.add(Movie.class);
        init(clazzz, ALL_PUs_UNDER_TEST);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        setUpInternal();
    }

    @Test
    public void testCRUD()
    {
        tryOperation(ALL_PUs_UNDER_TEST);
    }
    
    @Override
    protected void insert()
    {
        populateActors();                
        dao.insertActors(actors);
    }

    @Override
    protected void find()
    {
        
        Actor actor1 = (Actor) dao.find(Actor.class, 1);
        Actor actor2 = (Actor) dao.find(Actor.class, 2);
        
        assertActors(actor1, actor2);
    }

    @Override
    protected void update()
    {
        Actor actor1 = (Actor) dao.find(Actor.class, 1);
        Actor actor2 = (Actor) dao.find(Actor.class, 2);
        actor1.setName("Amresh");
        actor2.setName("Amir");
        dao.merge(actor1);
        dao.merge(actor2);
        Actor actor1Modified = (Actor) dao.find(Actor.class, 1);
        Actor actor2Modified = (Actor) dao.find(Actor.class, 2);
        assertUpdatedActors(actor1Modified, actor2Modified);        
    }

    @Override
    protected void remove()
    {        
        Actor actor1 = (Actor) dao.find(Actor.class, 1);
        Actor actor2 = (Actor) dao.find(Actor.class, 2);
        
        dao.remove(actor1);
        dao.remove(actor2);
        
        Actor actor1Deleted = (Actor) dao.find(Actor.class, 1);
        Actor actor2Deleted = (Actor) dao.find(Actor.class, 2);
        
        Assert.assertNull(actor1Deleted);
        Assert.assertNull(actor2Deleted);
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {        
    }
   
    @Override
    protected void loadDataForActor() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {

        KsDef ksDef = null;

        CfDef cfDef = new CfDef();
        cfDef.name = ACTOR;
        cfDef.keyspace = KEYSPACE;
        // cfDef.column_type = "Super";

        cfDef.setComparator_type("UTF8Type");
        cfDef.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("ACTOR_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef);

        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("ADDRESS_ID".getBytes()), "IntegerType");
        columnDef1.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef1);

        // ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("PERSON_ID"
        // .getBytes()), "IntegerType");
        // cfDef.addToColumn_metadata(columnDef2);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(cfDef);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace(KEYSPACE);
            CassandraCli.client.set_keyspace(KEYSPACE);

            List<CfDef> cfDefn = ksDef.getCf_defs();

            // CassandraCli.client.set_keyspace("KunderaTests");
            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase(ACTOR))
                {

                    CassandraCli.client.system_drop_column_family(MOVIE);

                }
            }
            CassandraCli.client.system_add_column_family(cfDef);

        }
        catch (NotFoundException e)
        {

            addKeyspace(ksDef, cfDefs);
        }

        CassandraCli.client.set_keyspace(KEYSPACE);
    }

    @Override
    protected void loadDataForMovie() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        KsDef ksDef = null;
        CfDef cfDef2 = new CfDef();

        cfDef2.name = MOVIE;
        cfDef2.keyspace = KEYSPACE;

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(cfDef2);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace(KEYSPACE);
            CassandraCli.client.set_keyspace(KEYSPACE);
            List<CfDef> cfDefss = ksDef.getCf_defs();

            for (CfDef cfDef : cfDefss)
            {
                if (cfDef.getName().equalsIgnoreCase(MOVIE))
                {
                    CassandraCli.client.system_drop_column_family(MOVIE);
                }
            }
            CassandraCli.client.system_add_column_family(cfDef2);
        }
        catch (NotFoundException e)
        {
            addKeyspace(ksDef, cfDefs);
        }
        CassandraCli.client.set_keyspace(KEYSPACE);

    }
    
    private void populateActors()
    {
        //Actors
        Actor actor1 = new Actor(1, "Tom Cruise");
        Actor actor2 = new Actor(2, "Emmanuelle Béart");     
        
        //Movies
        Movie movie1 = new Movie("m1", "War of the Worlds", 2005);
        Movie movie2 = new Movie("m2", "Mission Impossible", 1996);       
        Movie movie3 = new Movie("m3", "Hell", 2005);
        
        //Roles
        Role role1 = new Role("Ray Ferrier", "Lead Actor"); role1.setActor(actor1); role1.setMovie(movie1);
        Role role2 = new Role("Ethan Hunt", "Lead Actor"); role2.setActor(actor1); role2.setMovie(movie2);
        Role role3 = new Role("Claire Phelps", "Lead Actress"); role3.setActor(actor2); role1.setMovie(movie2);
        Role role4 = new Role("Sophie", "Supporting Actress"); role4.setActor(actor2); role1.setMovie(movie3);
        
        //Relationships
        actor1.addMovie(role1, movie1); actor1.addMovie(role2, movie2);
        actor2.addMovie(role3, movie2); actor2.addMovie(role4, movie3);
        
        movie1.addActor(role1, actor1);
        movie2.addActor(role2, actor1); movie2.addActor(role3, actor2);
        movie3.addActor(role4, actor2);
        
        actors.add(actor1);
        actors.add(actor2);
    }
    
    private void assertActors(Actor actor1, Actor actor2)
    {
        Assert.assertNotNull(actor1);
        Assert.assertEquals(1, actor1.getId());
        Assert.assertEquals("Tom Cruise", actor1.getName());
        Map<Role, Movie> movies1 = actor1.getMovies();
        Assert.assertFalse(movies1 == null || movies1.isEmpty());
        Assert.assertEquals(2, movies1.size());
        
        
        Assert.assertNotNull(actor2);
        Assert.assertEquals(2, actor2.getId());
        Assert.assertEquals("Emmanuelle Béart", actor2.getName());
        Map<Role, Movie> movies2 = actor2.getMovies();
        Assert.assertFalse(movies2 == null || movies2.isEmpty());
        Assert.assertEquals(2, movies2.size());
    }
    
    /**
     * @param actor1
     * @param actor2
     */
    private void assertUpdatedActors(Actor actor1, Actor actor2)
    {
        Assert.assertNotNull(actor1);
        Assert.assertEquals(1, actor1.getId());
        Assert.assertEquals("Amresh", actor1.getName());
        Map<Role, Movie> movies1 = actor1.getMovies();
        Assert.assertFalse(movies1 == null || movies1.isEmpty());
        Assert.assertEquals(2, movies1.size());
        
        Assert.assertNotNull(actor2);
        Assert.assertEquals(2, actor2.getId());
        Assert.assertEquals("Amir", actor2.getName());
        Map<Role, Movie> movies2 = actor2.getMovies();
        Assert.assertFalse(movies2 == null || movies2.isEmpty());
        Assert.assertEquals(2, movies2.size());
    }  
 }
