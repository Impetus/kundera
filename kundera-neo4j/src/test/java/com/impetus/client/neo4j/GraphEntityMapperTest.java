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
package com.impetus.client.neo4j;

import java.io.File;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.util.FileUtils;

import com.impetus.client.neo4j.imdb.Actor;
import com.impetus.client.neo4j.imdb.Movie;
import com.impetus.client.neo4j.imdb.Role;
import com.impetus.client.neo4j.index.Neo4JIndexManager;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Test case for {@link GraphEntityMapper}
 * 
 * @author amresh.singh
 */
public class GraphEntityMapperTest
{
    static EntityManagerFactory emf;

    static EntityManager em;

    static Neo4JClient client;

    GraphEntityMapper mapper;

    GraphDatabaseService graphDb;

    final static String PU = "imdb";

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PU);
        em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        client = (Neo4JClient) clients.get(PU);
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(PU);
        String datastoreFilePath = puMetadata.getProperty(PersistenceProperties.KUNDERA_DATASTORE_FILE_PATH);

        em.close();
        emf.close();

        if (datastoreFilePath != null)
            FileUtils.deleteRecursively(new File(datastoreFilePath));
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        mapper = new GraphEntityMapper(new Neo4JIndexManager());
        graphDb = client.getConnection();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        mapper = null;
        graphDb = null;
    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.GraphEntityMapper#getNodeFromEntity(java.lang.Object, Object, org.neo4j.graphdb.GraphDatabaseService, com.impetus.kundera.metadata.model.EntityMetadata, boolean)}
     * .
     */
    @Test
    public void testGetNodeFromEntity()
    {
        Actor actor = new Actor();
        actor.setId(1);
        actor.setName("Keenu Reeves");

        Transaction tx = graphDb.beginTx();
        Node node = mapper.getNodeFromEntity(actor, 1, graphDb, KunderaMetadataManager.getEntityMetadata(Actor.class),
                false);
        Assert.assertNotNull(node);
        Assert.assertEquals(1, node.getProperty("ACTOR_ID"));
        Assert.assertEquals("Keenu Reeves", node.getProperty("ACTOR_NAME"));

        node.delete();

        tx.success();
        tx.finish();
    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.GraphEntityMapper#createProxyNode(java.lang.Object, java.lang.Object, org.neo4j.graphdb.GraphDatabaseService, com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.metadata.model.EntityMetadata)}
     * .
     */
    @Test
    public void testCreateProxyNode()
    {
        EntityMetadata sourceM = KunderaMetadataManager.getEntityMetadata(Actor.class);
        EntityMetadata targetM = KunderaMetadataManager.getEntityMetadata(Movie.class);

        Transaction tx = graphDb.beginTx();
        Node proxyNode = mapper.createProxyNode(1, "A", graphDb, sourceM, targetM);

        Assert.assertNotNull(proxyNode);
        Assert.assertEquals(1, proxyNode.getProperty("ACTOR_ID"));
        Assert.assertEquals("A", proxyNode.getProperty("MOVIE_ID"));
        Assert.assertEquals("$PROXY_NODE$", proxyNode.getProperty("$NODE_TYPE$"));

        proxyNode.delete();
        tx.success();
        tx.finish();
    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.GraphEntityMapper#getEntityFromNode(org.neo4j.graphdb.Node, com.impetus.kundera.metadata.model.EntityMetadata)}
     * .
     */
    @Test
    public void testGetEntityFromNode()
    {
        Transaction tx = graphDb.beginTx();
        Node node = graphDb.createNode();
        node.setProperty("ACTOR_ID", 1);
        node.setProperty("ACTOR_NAME", "Amresh Singh");

        Actor actor = (Actor) mapper.getEntityFromNode(node, KunderaMetadataManager.getEntityMetadata(Actor.class));

        Assert.assertNotNull(actor);
        Assert.assertEquals(1, actor.getId());
        Assert.assertEquals("Amresh Singh", actor.getName());

        node.delete();
        tx.success();
        tx.finish();

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.GraphEntityMapper#getEntityFromRelationship(org.neo4j.graphdb.Relationship, com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.metadata.model.Relation)}
     * .
     */
    @Test
    public void testGetEntityFromRelationship()
    {
        Transaction tx = graphDb.beginTx();
        Node sourceNode = graphDb.createNode();
        sourceNode.setProperty("ACTOR_ID", 1);
        sourceNode.setProperty("ACTOR_NAME", "Amresh Singh");

        Node targetNode = graphDb.createNode();
        targetNode.setProperty("MOVIE_ID", "M1");
        targetNode.setProperty("TITLE", "Matrix Reloaded");
        targetNode.setProperty("YEAR", 1999);

        Relationship rel = sourceNode.createRelationshipTo(targetNode, DynamicRelationshipType.withName("ACTS_IN"));
        rel.setProperty("ROLE_NAME", "Neo");
        rel.setProperty("ROLE_TYPE", "Lead Actor");

        Role role = (Role) mapper.getEntityFromRelationship(rel, KunderaMetadataManager.getEntityMetadata(Actor.class),
                KunderaMetadataManager.getEntityMetadata(Actor.class).getRelation("movies"));

        Assert.assertNotNull(role);
        Assert.assertEquals("Neo", role.getRoleName());
        Assert.assertEquals("Lead Actor", role.getRoleType());

        rel.delete();
        sourceNode.delete();
        targetNode.delete();

        tx.success();
        tx.finish();
    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.GraphEntityMapper#createNodeProperties(java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata)}
     * .
     */
    @Test
    public void testCreateNodeProperties()
    {
        Actor actor = new Actor();
        actor.setId(1);
        actor.setName("Keenu Reeves");

        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(Actor.class);

        Map<String, Object> props = mapper.createNodeProperties(actor, m);
        Assert.assertNotNull(props);
        Assert.assertFalse(props.isEmpty());
        Assert.assertEquals(1, props.get("ACTOR_ID"));
        Assert.assertEquals("Keenu Reeves", props.get("ACTOR_NAME"));

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.GraphEntityMapper#populateRelationshipProperties(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.metadata.model.EntityMetadata, org.neo4j.graphdb.Relationship, java.lang.Object)}
     * .
     */
    @Test
    public void testPopulateRelationshipProperties()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.GraphEntityMapper#createRelationshipProperties(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.metadata.model.EntityMetadata, java.lang.Object)}
     * .
     */
    @Test
    public void testCreateRelationshipProperties()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.GraphEntityMapper#toNeo4JProperty(java.lang.Object)}
     * .
     */
    @Test
    public void testToNeo4JProperty()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.GraphEntityMapper#fromNeo4JObject(java.lang.Object, java.lang.reflect.Field)}
     * .
     */
    @Test
    public void testFromNeo4JObject()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.GraphEntityMapper#searchNode(java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata, org.neo4j.graphdb.GraphDatabaseService, boolean)}
     * .
     */
    @Test
    public void testSearchNode()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.GraphEntityMapper#getMatchingNodeFromIndexHits(org.neo4j.graphdb.index.IndexHits, boolean)}
     * .
     */
    @Test
    public void testGetMatchingNodeFromIndexHits()
    {

    }

}