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
package com.impetus.kundera.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.CoreTestClient;
import com.impetus.kundera.client.CoreTestClientFactory;
import com.impetus.kundera.configure.ClientFactoryConfiguraton;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.Employe;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.KunderaUser;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.IndexProcessor;
import com.impetus.kundera.metadata.processor.TableProcessor;

/**
 * @author vivek.mishra
 * 
 *         junit test case for HostConfiguration.
 * 
 */
public class HostconfigurationTest
{
    /* persistence unit. */
    private String persistenceUnit = "metaDataTest";

    /**
     * on setup
     */
    @Before
    public void setUp()
    {
        getEntityManagerFactory("create");
    }

    /**
     * test method.
     */
    @Test
    public void test()
    {
        CoreHostConfiguration hostConfiguration = new CoreHostConfiguration(null, null, persistenceUnit);
        Assert.assertNotNull(hostConfiguration.hosts);
        Assert.assertNotNull(hostConfiguration.port);
        Assert.assertTrue(hostConfiguration.getHosts().isEmpty());
        Assert.assertEquals("localhost", hostConfiguration.hosts);
        Assert.assertEquals("9160", hostConfiguration.port);

        try
        {
            hostConfiguration.onValidation(null, null);
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNotNull(iaex.getMessage());
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
