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
package com.impetus.kundera.configure.schema.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.CoreTestClient;
import com.impetus.kundera.client.CoreTestClientFactory;
import com.impetus.kundera.configure.ClientFactoryConfiguraton;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.Employe;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.KunderaUser;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

public class SchemaManagerTest
{

    private String persistenceUnit = "metaDataTest";

    @Before
    public void setUp()
    {
        getEntityManagerFactory("create");
    }
    
    @Test
    public void testCreate()
    {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        EntityManagerFactory  emf = Persistence.createEntityManagerFactory("metaDataTest", props);
        Assert.assertTrue(CoreSchemaManager.validateAction("create"));
        emf.close();
        Assert.assertTrue(CoreSchemaManager.validateAction("create"));
        Assert.assertFalse(CoreSchemaManager.validateAction("drop"));
    }

    @Test
    public void testCreateDrop()
    {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create-drop");
        EntityManagerFactory  emf = Persistence.createEntityManagerFactory("metaDataTest", props);
        Assert.assertTrue(CoreSchemaManager.validateAction("create-drop"));
        Assert.assertFalse(CoreSchemaManager.validateAction("create"));

        emf.close();
        Assert.assertFalse(CoreSchemaManager.validateAction("create-drop"));
        Assert.assertTrue(CoreSchemaManager.validateAction("drop"));
    }

    @Test
    public void testUpdate()
    {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "update");
        EntityManagerFactory  emf = Persistence.createEntityManagerFactory("metaDataTest", props);
        Assert.assertFalse(CoreSchemaManager.validateAction("create-drop"));
        Assert.assertFalse(CoreSchemaManager.validateAction("create"));
        Assert.assertTrue(CoreSchemaManager.validateAction("update"));
        emf.close();
        Assert.assertTrue(CoreSchemaManager.validateAction("update"));
        Assert.assertFalse(CoreSchemaManager.validateAction("drop"));
    }

    @After
    public void tearDown()
    {
        
    }
    /**
     * Gets the entity manager factory.
     * 
     * @param useLucene
     * @param property
     * 
     * @return the entity manager factory
     */
    private void getEntityManagerFactory(final String schemaProperty)
    {
        ClientMetadata clientMetadata = new ClientMetadata();
        Map<String, Object> props = new HashMap<String, Object>();

        props.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY, CoreTestClientFactory.class.getName());
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaMetaDataTest");
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, schemaProperty);
        clientMetadata.setLuceneIndexDir(null);

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
        clazzToPu.put(Employe.class.getName(), pus);
        clazzToPu.put(KunderaUser.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);


        MetadataBuilder metadataBuilder = new MetadataBuilder(persistenceUnit, CoreTestClient.class.getSimpleName(), null);


        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(Employe.class, metadataBuilder.buildEntityMetadata(Employe.class));
        metaModel.addEntityMetadata(KunderaUser.class, metadataBuilder.buildEntityMetadata(KunderaUser.class));

        appMetadata.getMetamodelMap().put(persistenceUnit, metaModel);

        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(persistenceUnit).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(persistenceUnit).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(persistenceUnit).getMappedSuperClassTypes());

//        KunderaMetadata.INSTANCE.addClientMetadata(persistenceUnit, clientMetadata);

        String[] persistenceUnits = new String[] { persistenceUnit };
        new ClientFactoryConfiguraton(null, persistenceUnits).configure();

        new SchemaConfiguration(null, persistenceUnits).configure();
    }

}
