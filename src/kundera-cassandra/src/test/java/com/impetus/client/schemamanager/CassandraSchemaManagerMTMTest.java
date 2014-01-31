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

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author Kuldeep.Kumar
 * 
 */
public class CassandraSchemaManagerMTMTest
{
    private static final String _keyspace = "KunderaExamples";

    private static final String _persistenceUnit = "cassandra";

    /**
     * @throws java.lang.Exception
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
        CassandraCli.dropKeySpace(_keyspace);
    }

    @Test
    public void test()
    {
        try
        {
            getEntityManagerFactory("create");
            Assert.assertTrue(CassandraCli.keyspaceExist(_keyspace));
            Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonnelUniMToM", _keyspace));
            Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityHabitatUniMToM", _keyspace));
            Assert.assertTrue(CassandraCli.columnFamilyExist("PERSONNEL_ADDRESS", _keyspace));
        }
        catch (InvalidEntityDefinitionException iedex)
        {
            Assert.assertEquals("It's manadatory to use @JoinTable with parent side of ManyToMany relationship.",
                    iedex.getMessage());
        }
    }

    /**
     * Gets the entity manager factory.
     * 
     * @param useLucene
     * @param property
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory(String property)
    {
        Map propertyMap = new HashMap();
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, property);
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(_persistenceUnit, propertyMap);
        return (EntityManagerFactoryImpl) emf;
    }
}
