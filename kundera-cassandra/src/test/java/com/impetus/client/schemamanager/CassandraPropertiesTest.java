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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.pelops.PelopsClientFactory;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.client.schemamanager.entites.Doctor;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author Kuldeep.Mishra
 * 
 */
public class CassandraPropertiesTest
{
    private static final String HOME_IMPADMIN_LUCENE = "/home/impadmin/lucene";

    private static final String KUNDERA_CASSANDRA_PROPERTIES = "kundera-cassandra.properties";

    /** The configuration. */
    private SchemaConfiguration configuration;

    /**
     * cassandra client
     */
    private Cassandra.Client client;

    /**
     * keyspace to create.
     */
    private String keyspace = "KunderaExamplesTests1";

    /**
     * persistence unit pu.
     */
    private String pu = "CassandraPropertiesTest";

    /**
     * useLucene
     */
    private final boolean useLucene = true;

    private org.apache.commons.logging.Log log = LogFactory.getLog(CassandraPropertiesTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        client = CassandraCli.getClient();
        configuration = new SchemaConfiguration(pu);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace(keyspace);
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
    }

    @Test
    public void testValid() throws NotFoundException, InvalidRequestException, TException, IOException
    {
        getEntityManagerFactory("create");

        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);
        Properties properties = new Properties();
        try
        {
            InputStream inStream = puMetadata != null ? ClassLoader.getSystemResourceAsStream(puMetadata
                    .getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY)) : null;
            properties.load(inStream);
            String expected_replication = properties.getProperty(Constants.REPLICATION_FACTOR);
            String expected_strategyClass = properties.getProperty(Constants.PLACEMENT_STRATEGY);

            KsDef ksDef = client.describe_keyspace(keyspace);
            Assert.assertEquals(expected_replication, ksDef.strategy_options.get("replication_factor"));
            Assert.assertEquals(expected_strategyClass, ksDef.getStrategy_class());
        }
        catch (IOException e)
        {
            log.warn("kundera-cassandra.properties file not found");
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
        ClientMetadata clientMetadata = new ClientMetadata();
        Map<String, Object> props = new HashMap<String, Object>();
        // String pu = pu;
        props.put(Constants.PERSISTENCE_UNIT_NAME, pu);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY, PelopsClientFactory.class.getName());
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, keyspace);
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, property);

        props.put(PersistenceProperties.KUNDERA_CLIENT_PROPERTY, KUNDERA_CASSANDRA_PROPERTIES);

        if (useLucene)
        {
            props.put(PersistenceProperties.KUNDERA_INDEX_HOME_DIR, HOME_IMPADMIN_LUCENE);

            clientMetadata.setLuceneIndexDir(HOME_IMPADMIN_LUCENE);
        }
        else
        {

            clientMetadata.setLuceneIndexDir(null);
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
        clazzToPu.put(Doctor.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(Doctor.class);

        TableProcessor processor = new TableProcessor();
        processor.process(Doctor.class, m);

        m.setPersistenceUnit(pu);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(Doctor.class, m);

        appMetadata.getMetamodelMap().put(pu, metaModel);
        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder().getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder().getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder().getMappedSuperClassTypes());

        KunderaMetadata.INSTANCE.addClientMetadata(pu, clientMetadata);
        CassandraPropertyReader reader = new CassandraPropertyReader();
        reader.read(pu);
        configuration.configure();
        return null;
    }
}
