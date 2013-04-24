/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.oraclenosql.entities.PersonOTOOracleNoSQL;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.persistence.context.jointable.JoinTableData.OPERATION;

/**
 * Test case for {@link OracleNoSQLClient}
 * 
 * @author amresh.singh
 */
public class OracleNoSQLClientTest
{

    /** The Constant REDIS_PU. */
    private static final String PU = "twikvstore";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(OracleNoSQLClient.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PU);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

    @Test
    public void testPersistJoinTableData()
    {

        final String schemaName = "KunderaTests";
        final String tableName = "PERSON_ADDRESS";
        final String joinColumn = "PERSON_ID";
        final String inverseJoinColumn = "ADDRESS_ID";

        JoinTableData joinTableData = new JoinTableData(OPERATION.INSERT, schemaName, tableName, joinColumn,
                inverseJoinColumn, PersonOTOOracleNoSQL.class);

        String joinKey1 = "JK1";
        Integer joinKey2 = new Integer(2);

        Integer inverseJoinKey1 = new Integer(1);
        Double inverseJoinKey2 = new Double(2.2);
        String inverseJoinKey3 = "IJK3";

        Set inverseJoinKeysFor1 = new HashSet();
        inverseJoinKeysFor1.add(inverseJoinKey1);
        inverseJoinKeysFor1.add(inverseJoinKey2);

        Set inverseJoinKeysFor2 = new HashSet();
        inverseJoinKeysFor2.add(inverseJoinKey2);
        inverseJoinKeysFor2.add(inverseJoinKey3);

        joinTableData.addJoinTableRecord(joinKey1, inverseJoinKeysFor1);
        joinTableData.addJoinTableRecord(joinKey2, inverseJoinKeysFor2);

        EntityManager em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        OracleNoSQLClient client = (OracleNoSQLClient) clients.get(PU);
        client.persistJoinTable(joinTableData);

        List<String> columns = client.getColumnsById(schemaName, tableName, joinColumn, inverseJoinColumn, joinKey1,
                String.class);

        Assert.assertNotNull(columns);
        Assert.assertEquals(true, !columns.isEmpty());
        Assert.assertEquals(2, columns.size());
        Assert.assertEquals(true, columns.contains(inverseJoinKey1.toString()));
        Assert.assertEquals(true, columns.contains(inverseJoinKey2.toString()));

        Object[] ids = client.findIdsByColumn(schemaName, tableName, joinColumn, inverseJoinColumn, inverseJoinKey2,
                PersonOTOOracleNoSQL.class);
        Assert.assertNotNull(ids);
        Assert.assertTrue(ids.length == 2);

        client.deleteByColumn(schemaName, tableName, inverseJoinColumn, inverseJoinKey1);
        client.deleteByColumn(schemaName, tableName, inverseJoinColumn, inverseJoinKey2);

        columns = client.getColumnsById(schemaName, tableName, joinColumn, inverseJoinColumn, joinKey1, String.class);

        Assert.assertTrue(columns.isEmpty());
    }
}
