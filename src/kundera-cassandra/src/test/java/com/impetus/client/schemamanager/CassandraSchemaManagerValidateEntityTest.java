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

import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.pelops.PelopsClientFactory;
import com.impetus.client.cassandra.schemamanager.CassandraSchemaManager;
import com.impetus.client.schemamanager.entites.InvalidCounterColumnEntity;
import com.impetus.client.schemamanager.entites.ValidCounterColumnFamily;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author impadmin
 * 
 */
public class CassandraSchemaManagerValidateEntityTest
{

    private String persistenceUnit = "cassandraProperties";

    // private String[] persistenceUnits = new String[] {persistenceUnit};

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
     * {@link com.impetus.client.cassandra.schemamanager.CassandraSchemaManager#validateEntity(java.lang.Class)}
     * .
     */
    @Test
    public void testValidateEntity()
    {
        EntityManagerFactoryImpl emf = getEntityManagerFactory();
        CassandraPropertyReader reader = new CassandraPropertyReader(null, emf.getKunderaMetadataInstance()
                .getApplicationMetadata().getPersistenceUnitMetadata("cassandraProperties"));
        reader.read(persistenceUnit);
        CassandraSchemaManager manager = new CassandraSchemaManager(PelopsClientFactory.class.getName(), null,
                emf.getKunderaMetadataInstance());
        boolean valid = manager.validateEntity(ValidCounterColumnFamily.class);
        Assert.assertTrue(valid);
        valid = manager.validateEntity(InvalidCounterColumnEntity.class);
        Assert.assertFalse(valid);
    }

    /**
     * Gets the entity manager factory.
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory()
    {
        return (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory("cassandraProperties");
    }
}
