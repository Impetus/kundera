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
package com.impetus.client.neo4j;

import java.io.File;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.kernel.impl.util.FileUtils;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * @author amresh
 * 
 */
public class Neo4JClientTest
{
    static EntityManagerFactory emf;

    static EntityManager em;

    static Neo4JClient client;

    GraphEntityMapper mapper;

    final static String PU = "neo4jTest";

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
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#persist(java.lang.Object, java.lang.Object, java.util.List)}
     * .
     */
    @Test
    public void testOnPersist()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#find(java.lang.Class, java.lang.Object)}
     * .
     */
    @Test
    public void testFindClassObject()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#findAll(java.lang.Class, java.lang.Object[])}
     * .
     */
    @Test
    public void testFindAll()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#delete(java.lang.Object, java.lang.Object)}
     * .
     */
    @Test
    public void testDelete()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#persistJoinTable(com.impetus.kundera.persistence.context.jointable.JoinTableData)}
     * .
     */
    @Test
    public void testPersistJoinTable()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#getColumnsById(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.Object)}
     * .
     */
    @Test
    public void testGetColumnsById()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#findIdsByColumn(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.Object, java.lang.Class)}
     * .
     */
    @Test
    public void testFindIdsByColumn()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#deleteByColumn(java.lang.String, java.lang.String, java.lang.String, java.lang.Object)}
     * .
     */
    @Test
    public void testDeleteByColumn()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#findByRelation(java.lang.String, java.lang.Object, java.lang.Class)}
     * .
     */
    @Test
    public void testFindByRelation()
    {

    }

    /**
     * Test method for {@link com.impetus.client.neo4j.Neo4JClient#getReader()}.
     */
    @Test
    public void testGetReader()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#getQueryImplementor()}.
     */
    @Test
    public void testGetQueryImplementor()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#addBatch(com.impetus.kundera.graph.Node)}
     * .
     */
    @Test
    public void testAddBatch()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#executeBatch()}.
     */
    @Test
    public void testExecuteBatch()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#getBatchInserter()}.
     */
    @Test
    public void testGetBatchInserter()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#bind(com.impetus.kundera.persistence.TransactionResource)}
     * .
     */
    @Test
    public void testBind()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#executeLuceneQuery(com.impetus.kundera.metadata.model.EntityMetadata, java.lang.String)}
     * .
     */
    @Test
    public void testExecuteLuceneQuery()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClient#getConnection()}.
     */
    @Test
    public void testGetConnection()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClientBase#isEntityForNeo4J(com.impetus.kundera.metadata.model.EntityMetadata)}
     * .
     */
    @Test
    public void testIsEntityForNeo4J()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClientBase#populateBatchSize(java.lang.String, java.util.Map)}
     * .
     */
    @Test
    public void testPopulateBatchSize()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.neo4j.Neo4JClientBase#getBatchSize()}.
     */
    @Test
    public void testGetBatchSize()
    {

    }

    /**
     * Test method for {@link com.impetus.client.neo4j.Neo4JClientBase#clear()}.
     */
    @Test
    public void testClear()
    {

    }

}
