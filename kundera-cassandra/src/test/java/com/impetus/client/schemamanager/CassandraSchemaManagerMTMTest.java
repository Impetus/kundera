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

import com.impetus.client.cassandra.CassandraClientFactory;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.client.schemamanager.entites.CassandraEntityHabitatUniMToM;
import com.impetus.client.schemamanager.entites.CassandraEntityPersonnelUniMToM;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientFactoryConfiguraton;
import com.impetus.kundera.configure.SchemaConfiguration;
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
    private static final String _keyspace = "KunderaCassandraMTMExamples";

    private static final String _persistenceUnit = "cassandra";

    /** The configuration. */
    private SchemaConfiguration configuration;

    private final boolean useLucene = false;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        configuration = new SchemaConfiguration(null,_persistenceUnit);
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
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(Constants.PERSISTENCE_UNIT_NAME, _persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY, CassandraClientFactory.class.getName());
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, _keyspace);
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, property);
        if (useLucene)
        {
            props.put(PersistenceProperties.KUNDERA_INDEX_HOME_DIR, "/home/impadmin/lucene");
        }
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(_persistenceUnit);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put(_persistenceUnit, puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(_persistenceUnit);
        clazzToPu.put(CassandraEntityPersonnelUniMToM.class.getName(), pus);
        clazzToPu.put(CassandraEntityHabitatUniMToM.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(CassandraEntityPersonnelUniMToM.class);
        EntityMetadata m1 = new EntityMetadata(CassandraEntityHabitatUniMToM.class);

        TableProcessor processor = new TableProcessor(null);
        processor.process(CassandraEntityPersonnelUniMToM.class, m);
        processor.process(CassandraEntityHabitatUniMToM.class, m1);

        m.setPersistenceUnit(_persistenceUnit);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(CassandraEntityPersonnelUniMToM.class, m);
        metaModel.addEntityMetadata(CassandraEntityHabitatUniMToM.class, m1);

        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(_persistenceUnit).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(_persistenceUnit).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(_persistenceUnit).getMappedSuperClassTypes());

        appMetadata.getMetamodelMap().put(_persistenceUnit, metaModel);

        new ClientFactoryConfiguraton(null,_persistenceUnit).configure();
        configuration.configure();
        return null;
    }
}
