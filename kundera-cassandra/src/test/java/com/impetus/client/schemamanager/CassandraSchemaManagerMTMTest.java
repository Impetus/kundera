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

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.pelops.PelopsClientFactory;
import com.impetus.client.cassandra.schemamanager.CassandraSchemaManager;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.client.schemamanager.entites.CassandraEntityHabitatUniMToM;
import com.impetus.client.schemamanager.entites.CassandraEntityPersonnelUniMToM;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientFactoryConfiguraton;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.configure.schema.api.SchemaManager;
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
public class CassandraSchemaManagerMTMTest
{
    /** The configuration. */
    private SchemaConfiguration configuration;

    /** Configure schema manager. */
    private SchemaManager schemaManager;

    private Cassandra.Client client;

    private final boolean useLucene = false;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        configuration = new SchemaConfiguration("CassandraSchemaManagerMTMTest");
        CassandraCli.cassandraSetUp();
        CassandraCli cli = new CassandraCli();
        client = cli.getClient();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        try
        {
            client.system_drop_keyspace("KunderaCassandraMTMTest");
        }
        catch (InvalidRequestException irex)
        {
            Assert.assertTrue(!CassandraCli.keyspaceExist("KunderaCassandraMTMTest"));
        }
    }

    @Test
    public void test()
    {
        try
        {
            getEntityManagerFactory("create");

            schemaManager = new CassandraSchemaManager(PelopsClientFactory.class.getName());
            schemaManager.exportSchema();
            Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCassandraMTMTest"));
            Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonnelUniMToM",
                    "KunderaCassandraMTMTest"));
            Assert.assertTrue(CassandraCli
                    .columnFamilyExist("CassandraEntityHabitatUniMToM", "KunderaCassandraMTMTest"));
            Assert.assertTrue(CassandraCli.columnFamilyExist("PERSONNEL_ADDRESS", "KunderaCassandraMTMTest"));
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
        String persistenceUnit = "CassandraSchemaManagerMTMTest";
        props.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY, PelopsClientFactory.class.getName());
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaCassandraMTMTest");
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, property);
        if (useLucene)
        {
            props.put(PersistenceProperties.KUNDERA_INDEX_HOME_DIR, "/home/impadmin/lucene");
        }
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(persistenceUnit);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put("CassandraSchemaManagerMTMTest", puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(persistenceUnit);
        clazzToPu.put(CassandraEntityPersonnelUniMToM.class.getName(), pus);
        clazzToPu.put(CassandraEntityHabitatUniMToM.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(CassandraEntityPersonnelUniMToM.class);
        EntityMetadata m1 = new EntityMetadata(CassandraEntityHabitatUniMToM.class);

        TableProcessor processor = new TableProcessor();
        processor.process(CassandraEntityPersonnelUniMToM.class, m);
        processor.process(CassandraEntityHabitatUniMToM.class, m1);

        m.setPersistenceUnit(persistenceUnit);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(CassandraEntityPersonnelUniMToM.class, m);
        metaModel.addEntityMetadata(CassandraEntityHabitatUniMToM.class, m1);

        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder().getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder().getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder().getMappedSuperClassTypes());

        appMetadata.getMetamodelMap().put(persistenceUnit, metaModel);

        new ClientFactoryConfiguraton(persistenceUnit).configure();
        configuration.configure();
        // EntityManagerFactoryImpl impl = new
        // EntityManagerFactoryImpl(puMetadata, props);
        return null;
    }
}
