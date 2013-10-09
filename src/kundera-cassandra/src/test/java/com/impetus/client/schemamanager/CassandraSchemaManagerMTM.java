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

import com.impetus.client.cassandra.thrift.ThriftClientFactory;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.client.schemamanager.entites.CassandraEntityHabitatUniMToM;
import com.impetus.client.schemamanager.entites.CassandraEntityPersonnelUniMToM;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientFactoryConfiguraton;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.TableProcessor;
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
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(Constants.PERSISTENCE_UNIT_NAME, pu);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY,
                "com.impetus.client.cassandra.pelops.PelopsClientFactory");
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, keyspace);
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, property);
        if (useLucene)
        {
            props.put(PersistenceProperties.KUNDERA_INDEX_HOME_DIR, "/home/impadmin/lucene");
        }
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(pu);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put(pu, puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(pu);
        clazzToPu.put(CassandraEntityPersonnelUniMToM.class.getName(), pus);
        clazzToPu.put(CassandraEntityHabitatUniMToM.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(CassandraEntityPersonnelUniMToM.class);
        EntityMetadata m1 = new EntityMetadata(CassandraEntityHabitatUniMToM.class);

        TableProcessor processor = new TableProcessor(null);
        processor.process(CassandraEntityPersonnelUniMToM.class, m);
        processor.process(CassandraEntityHabitatUniMToM.class, m1);

        m.setPersistenceUnit(pu);

        MetadataBuilder metadataBuilder = new MetadataBuilder(pu, ThriftClientFactory.class.getSimpleName(), null);

        MetamodelImpl metaModel = new MetamodelImpl();

        metaModel.addEntityMetadata(CassandraEntityPersonnelUniMToM.class, metadataBuilder.buildEntityMetadata(CassandraEntityPersonnelUniMToM.class));
        metaModel.addEntityMetadata(CassandraEntityHabitatUniMToM.class, metadataBuilder.buildEntityMetadata(CassandraEntityHabitatUniMToM.class));

        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(pu).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(pu).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(pu).getMappedSuperClassTypes());

        appMetadata.getMetamodelMap().put(pu, metaModel);

        new ClientFactoryConfiguraton(null, pu).configure();
        new SchemaConfiguration(null, pu).configure();
        // EntityManagerFactoryImpl impl = new
        // EntityManagerFactoryImpl(puMetadata, props);
        return null;
    }
}
