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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.pelops.PelopsClientFactory;
import com.impetus.client.cassandra.schemamanager.CassandraSchemaManager;
import com.impetus.client.schemamanager.entites.InvalidCounterColumnEntity;
import com.impetus.client.schemamanager.entites.ValidCounterColumnFamily;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.TableProcessor;
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
        getEntityManagerFactory();
        CassandraPropertyReader reader = new CassandraPropertyReader();
        reader.read(persistenceUnit);
        CassandraSchemaManager manager = new CassandraSchemaManager(PelopsClientFactory.class.getName());
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
        Map<String, Object> props = new HashMap<String, Object>();
        // String persistenceUnit = "cassandraProperties";
        props.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY,
                "com.impetus.client.cassandra.pelops.PelopsClientFactory");
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaCounterColumn");
        props.put(PersistenceProperties.KUNDERA_CLIENT_PROPERTY, "kundera-cassandra.properties");

        KunderaMetadata.INSTANCE.setApplicationMetadata(null);

        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(persistenceUnit);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put(persistenceUnit, puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(persistenceUnit);
        clazzToPu.put(ValidCounterColumnFamily.class.getName(), pus);
        clazzToPu.put(InvalidCounterColumnEntity.class.getName(), pus);
        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(ValidCounterColumnFamily.class);
        EntityMetadata m1 = new EntityMetadata(InvalidCounterColumnEntity.class);

        TableProcessor processor = new TableProcessor();
        processor.process(ValidCounterColumnFamily.class, m);
        processor.process(InvalidCounterColumnEntity.class, m1);
        m.setPersistenceUnit(persistenceUnit);
        m1.setPersistenceUnit(persistenceUnit);
        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(ValidCounterColumnFamily.class, m);
        metaModel.addEntityMetadata(InvalidCounterColumnEntity.class, m1);
        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder().getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder().getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder().getMappedSuperClassTypes());

        appMetadata.getMetamodelMap().put(persistenceUnit, metaModel);
        // EntityManagerFactoryImpl emf = new
        // EntityManagerFactoryImpl(persistenceUnit, props);
        // return emf;
        return null;
    }
}
