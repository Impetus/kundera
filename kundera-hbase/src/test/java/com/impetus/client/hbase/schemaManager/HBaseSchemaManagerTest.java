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
package com.impetus.client.hbase.schemaManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.HBaseClientFactory;
import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.client.hbase.schemamanager.HBaseSchemaManager;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * HbaseSchemaManagerTest class test the auto creation schema property in hbase
 * data store.
 * 
 * @author Kuldeep.Kumar
 * 
 */
public class HBaseSchemaManagerTest
{
    private final boolean useLucene = true;

    /** Configure schema manager. */
    private SchemaManager schemaManager;

    /** The pu metadata. */
    private PersistenceUnitMetadata puMetadata;

    /** The configuration. */
    private SchemaConfiguration configuration;

    /** The port. */
    private String port;

    /** The host. */
    private String host;

    /** The admin. */
    private static HBaseAdmin admin;

    private ApplicationMetadata appMetadata;

    private HBaseCli cli;

    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(HBaseSchemaManagerTest.class);

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        cli = new HBaseCli();
        logger.info("starting server");
        // if (!HBaseCli.isStarted)
        cli.startCluster();
        if (admin == null)
        {
            admin = cli.utility.getHBaseAdmin();
        }
        configuration = new SchemaConfiguration("hbase");

    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        // if (HBaseCli.isStarted)
        cli.stopCluster();
        appMetadata = null;
    }

    @Test
    public void testDu()
    {
        
    }
    /**
     * Test schema operation.
     */
//    @Test
    public void testSchemaOperation()
    {
        try
        {
            getEntityManagerFactory("create");
            schemaManager = new HBaseSchemaManager(HBaseClientFactory.class.getName());
            schemaManager.exportSchema();
            Assert.assertTrue(admin.isTableAvailable("HbaseEntitySimple"));
            Assert.assertTrue(admin.isTableAvailable("HbaseEntitySuper"));
            Assert.assertTrue(admin.isTableAvailable("HbaseEntityAddressUni1To1"));
            Assert.assertTrue(admin.isTableAvailable("HbaseEntityAddressUniMTo1"));
            Assert.assertTrue(admin.isTableAvailable("HbaseEntityAddressUni1ToM"));
            Assert.assertTrue(admin.isTableAvailable("HbaseEntityPersonUni1ToM"));
            Assert.assertTrue(admin.isTableAvailable("HbaseEntityPersonUni1To1"));
            Assert.assertTrue(admin.isTableAvailable("HbaseEntityPersonUniMto1"));
            Assert.assertTrue(admin.isTableAvailable("HbaseEntityAddressUni1To1PK"));
            Assert.assertTrue(admin.isTableAvailable("HbaseEntityPersonUni1To1PK"));
        }
        catch (IOException e)
        {
            Assert.fail("Failed, Caused by:" + e.getMessage());
        }
        catch (Exception e)
        {
            Assert.fail("Failed, Caused by:" + e.getMessage());
        }
    }

    /**
     * Gets the entity manager factory.
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory(String property)
    {
        ClientMetadata clientMetadata = new ClientMetadata();
        Map<String, Object> props = new HashMap<String, Object>();
        String persistenceUnit = "hbase";
        props.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaHbaseExamples");
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY, "com.impetus.client.hbase.HBaseClientFactory");
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, property);

        if (useLucene)
        {
            props.put(PersistenceProperties.KUNDERA_INDEX_HOME_DIR, "/home/impadmin/lucene");

            clientMetadata.setLuceneIndexDir("/home/impadmin/lucene");
        }
        else
        {

            clientMetadata.setLuceneIndexDir(null);
        }

        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(persistenceUnit);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put("hbase", puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(persistenceUnit);
        clazzToPu.put(HBaseEntitySimple.class.getName(), pus);
        clazzToPu.put(HBaseEntitySuper.class.getName(), pus);
        clazzToPu.put(HBaseEntityAddressUni1To1.class.getName(), pus);
        clazzToPu.put(HBaseEntityAddressUni1ToM.class.getName(), pus);
        clazzToPu.put(HBaseEntityAddressUniMTo1.class.getName(), pus);
        clazzToPu.put(HBaseEntityPersonUniMto1.class.getName(), pus);
        clazzToPu.put(HBaseEntityPersonUni1To1.class.getName(), pus);
        clazzToPu.put(HBaseEntityPersonUni1ToM.class.getName(), pus);
        clazzToPu.put(HBaseEntityAddressUni1To1PK.class.getName(), pus);
        clazzToPu.put(HBaseEntityPersonUni1To1PK.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(HBaseEntitySimple.class);
        EntityMetadata m1 = new EntityMetadata(HBaseEntitySuper.class);
        EntityMetadata m2 = new EntityMetadata(HBaseEntityAddressUni1To1.class);
        EntityMetadata m3 = new EntityMetadata(HBaseEntityAddressUni1ToM.class);
        EntityMetadata m4 = new EntityMetadata(HBaseEntityAddressUniMTo1.class);
        EntityMetadata m5 = new EntityMetadata(HBaseEntityPersonUniMto1.class);
        EntityMetadata m6 = new EntityMetadata(HBaseEntityPersonUni1To1.class);
        EntityMetadata m7 = new EntityMetadata(HBaseEntityPersonUni1ToM.class);
        EntityMetadata m8 = new EntityMetadata(HBaseEntityPersonUni1To1PK.class);
        EntityMetadata m9 = new EntityMetadata(HBaseEntityAddressUni1To1PK.class);

        TableProcessor processor = new TableProcessor();
        processor.process(HBaseEntitySimple.class, m);
        processor.process(HBaseEntitySuper.class, m1);
        processor.process(HBaseEntityAddressUni1To1.class, m2);
        processor.process(HBaseEntityAddressUni1ToM.class, m3);
        processor.process(HBaseEntityAddressUniMTo1.class, m4);
        processor.process(HBaseEntityPersonUniMto1.class, m5);
        processor.process(HBaseEntityPersonUni1To1.class, m6);
        processor.process(HBaseEntityPersonUni1ToM.class, m7);
        processor.process(HBaseEntityPersonUni1To1PK.class, m8);
        processor.process(HBaseEntityAddressUni1To1PK.class, m9);

        m.setPersistenceUnit(persistenceUnit);
        m1.setPersistenceUnit(persistenceUnit);
        m2.setPersistenceUnit(persistenceUnit);
        m3.setPersistenceUnit(persistenceUnit);
        m4.setPersistenceUnit(persistenceUnit);
        m5.setPersistenceUnit(persistenceUnit);
        m6.setPersistenceUnit(persistenceUnit);
        m7.setPersistenceUnit(persistenceUnit);
        m8.setPersistenceUnit(persistenceUnit);
        m9.setPersistenceUnit(persistenceUnit);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(HBaseEntitySimple.class, m);
        metaModel.addEntityMetadata(HBaseEntitySuper.class, m1);
        metaModel.addEntityMetadata(HBaseEntityAddressUni1To1.class, m2);
        metaModel.addEntityMetadata(HBaseEntityAddressUni1ToM.class, m3);
        metaModel.addEntityMetadata(HBaseEntityAddressUniMTo1.class, m4);
        metaModel.addEntityMetadata(HBaseEntityPersonUniMto1.class, m5);
        metaModel.addEntityMetadata(HBaseEntityPersonUni1To1.class, m6);
        metaModel.addEntityMetadata(HBaseEntityPersonUni1ToM.class, m7);
        metaModel.addEntityMetadata(HBaseEntityPersonUni1To1PK.class, m8);
        metaModel.addEntityMetadata(HBaseEntityAddressUni1To1PK.class, m9);

        appMetadata.getMetamodelMap().put(persistenceUnit, metaModel);
        KunderaMetadata.INSTANCE.addClientMetadata(persistenceUnit, clientMetadata);

        configuration.configure();
        // EntityManagerFactoryImpl impl = new
        // EntityManagerFactoryImpl(puMetadata, props);
        return null;
    }
}
