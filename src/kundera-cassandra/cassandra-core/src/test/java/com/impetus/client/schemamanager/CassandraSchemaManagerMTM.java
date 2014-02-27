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

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author Kuldeep.Kumar
 * 
 */
public class CassandraSchemaManagerMTM
{
    private static final String keyspace = "KunderaCassandraExamples";

    private static final String pu = "cassandra";

    private final boolean useLucene = false;

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
        CassandraCli.dropKeySpace(keyspace);
    }

    @Test
    public void test()
    {
        try
        {
            getEntityManagerFactory("create");

            Assert.assertTrue(CassandraCli.keyspaceExist(keyspace));
            Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonnelUniMToM", keyspace));
            Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityHabitatUniMToM", keyspace));
            Assert.assertTrue(CassandraCli.columnFamilyExist("PERSONNEL_ADDRESS", keyspace));
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
        propertyMap.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaCassandraMTMExamples");
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("cassandra", propertyMap);
        return (EntityManagerFactoryImpl) emf;
    }
}
