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

package com.impetus.client.schemamanager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * CassandraSchemaManagerTest class test the auto creation schema property in
 * cassandra data store.
 * 
 * @author Kuldeep.Kumar
 * 
 */
public class CassandraSchemaManagerTest
{
    private static final String _PU = "CassandraSchemaManager";

    private static final String _KEYSPACE = "CassandraSchemaManagerTest";

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace(_KEYSPACE);
    }

    /**
     * Test schema operation.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws TException
     *             the t exception
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws UnavailableException
     *             the unavailable exception
     * @throws TimedOutException
     *             the timed out exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    @Test
    public void schemaOperation() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        EntityManagerFactory emf = getEntityManagerFactory(null, _PU);
        Assert.assertTrue(CassandraCli.keyspaceExist(_KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySuper", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUni1To1", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUniMTo1", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUni1ToM", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUniMto1", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUni1ToM", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUni1To1", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUni1To1PK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUni1To1PK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBi1To1FK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBi1To1PK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBi1ToM", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBiMTo1", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBi1To1FK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBi1To1PK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBi1ToM", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBiMTo1", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEmbeddedPersonUniMto1", _KEYSPACE));

        emf.close();

    }

    @Test
    public void testValidate()
    {
        try
        {
            final String pu = "CassandraSchemaOperationTest";
            Map propertyMap = new HashMap();
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
            propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
            EntityManagerFactory emf = getEntityManagerFactory(propertyMap, pu);

            String keyspaceName = "KunderaCoreExmples";
            CassandraCli.dropColumnFamily("CassandraEntitySimple", keyspaceName);
            String colFamilySql = "CREATE table \"CassandraEntitySimple\" (\"PERSON_ID\" varchar PRIMARY KEY,\"PERSON_NAME\" varchar, \"AGE\" int)";
            CassandraCli.executeCqlQuery(colFamilySql, keyspaceName);
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "validate");
            propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
            EntityManagerFactory emf1 = getEntityManagerFactory(propertyMap, pu);
            emf1.close();
            CassandraCli.dropKeySpace(keyspaceName);
        }
        catch (SchemaGenerationException sgex)
        {
            Assert.fail(sgex.getMessage());
        }

    }

    /**
     * Gets the entity manager factory.
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory(Map propertyMap, final String persistenceUnit)
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
        return (EntityManagerFactoryImpl) emf;
    }
}
