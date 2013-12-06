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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.client.hbase.HBaseClientFactory;
import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.configure.ClientFactoryConfiguraton;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author Kuldeep.Kumar
 * 
 */
public class HBaseSchemaOperationTest
{
    private static final String HBASE_ENTITY_SIMPLE = "HbaseEntitySimple";

    private static final String TABLE = "KunderaHbaseTests";

    /** The configuration. */
    private static SchemaConfiguration configuration;

    /** Configure schema manager. */
    private SchemaManager schemaManager;

    private final boolean useLucene = false;

    private static HBaseAdmin admin;

    private static HBaseCli cli;

    private String persistenceUnit = "HBaseSchemaOperationTest";

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        cli = new HBaseCli();
        cli.startCluster();
        if (admin == null)
        {
            admin = HBaseCli.utility.getHBaseAdmin();
        }
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        configuration = new SchemaConfiguration(null, "HBaseSchemaOperationTest");

    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        cli.dropTable(TABLE);
        HBaseCli.stopCluster();

        // admin = null;
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        // schemaManager.dropSchema();
        // HBaseCli.stopCluster();
        // admin = null;
    }

    @Test
    public void testCreate() throws IOException
    {
        getEntityManagerFactory("create");
        Assert.assertTrue(admin.isTableAvailable(TABLE));

        HTableDescriptor descriptor = admin.getTableDescriptor(TABLE.getBytes());
        Assert.assertNotNull(descriptor.getFamilies());
        Assert.assertEquals(1, descriptor.getFamilies().size());
        for (HColumnDescriptor columnDescriptor : descriptor.getFamilies())
        {
            Assert.assertNotNull(columnDescriptor);
            Assert.assertNotNull(columnDescriptor.getNameAsString());
            Assert.assertEquals(HBASE_ENTITY_SIMPLE, columnDescriptor.getNameAsString());
        }

        admin.disableTable(TABLE);
        admin.deleteTable(TABLE);
        Assert.assertFalse(admin.isTableAvailable(TABLE));
    }

    @Test
    public void testCreatedrop() throws IOException
    {
        getEntityManagerFactory("create-drop");
        // schemaManager = new
        // HBaseSchemaManager(HBaseClientFactory.class.getName(), null);
        // schemaManager.exportSchema();
        Assert.assertTrue(admin.isTableAvailable(TABLE));

        HTableDescriptor descriptor = admin.getTableDescriptor(TABLE.getBytes());
        Assert.assertNotNull(descriptor.getFamilies());
        Assert.assertEquals(1, descriptor.getFamilies().size());
        for (HColumnDescriptor columnDescriptor : descriptor.getFamilies())
        {
            Assert.assertNotNull(columnDescriptor);
            Assert.assertNotNull(columnDescriptor.getNameAsString());
            Assert.assertEquals(HBASE_ENTITY_SIMPLE, columnDescriptor.getNameAsString());
        }

        HBaseClientFactory clientFactory = (HBaseClientFactory) ClientResolver.getClientFactory(persistenceUnit);
        clientFactory.getSchemaManager(null).dropSchema();
        Assert.assertTrue(admin.isTableAvailable(TABLE));
        Assert.assertNull(admin.getTableDescriptor(TABLE.getBytes()).getFamily(HBASE_ENTITY_SIMPLE.getBytes()));
    }

    @Test
    public void testUpdate() throws IOException
    {
        HTableDescriptor descriptor1 = new HTableDescriptor(TABLE);
        HColumnDescriptor columnDescriptor1 = new HColumnDescriptor("PERSON_NAME");
        descriptor1.addFamily(columnDescriptor1);
        if (admin.isTableAvailable(TABLE))
        {
            admin.disableTable(TABLE);
            admin.deleteTable(TABLE);
        }
        admin.createTable(descriptor1);
        Assert.assertTrue(admin.isTableAvailable(TABLE));
        HTableDescriptor descriptor2 = admin.getTableDescriptor(TABLE.getBytes());
        Assert.assertNotNull(descriptor2.getFamilies());
        Assert.assertEquals(1, descriptor2.getFamilies().size());
        for (HColumnDescriptor columnDescriptor : descriptor2.getFamilies())
        {
            Assert.assertNotNull(columnDescriptor);
            Assert.assertNotNull(columnDescriptor.getNameAsString());
            Assert.assertEquals("PERSON_NAME", columnDescriptor.getNameAsString());
        }

        getEntityManagerFactory("update");
        // schemaManager = new
        // HBaseSchemaManager(HBaseClientFactory.class.getName(), null);
        // schemaManager.exportSchema();

        Assert.assertTrue(admin.isTableAvailable(TABLE));

        HTableDescriptor descriptor = admin.getTableDescriptor(TABLE.getBytes());
        Assert.assertNotNull(descriptor.getFamilies());
        Assert.assertEquals(2, descriptor.getFamilies().size());
        List<String> columns = new ArrayList<String>();
        columns.add(HBASE_ENTITY_SIMPLE);
        columns.add("PERSON_NAME");
        for (HColumnDescriptor columnDescriptor : descriptor.getFamilies())
        {
            Assert.assertNotNull(columnDescriptor);
            Assert.assertNotNull(columnDescriptor.getNameAsString());
            Assert.assertTrue(columns.contains(columnDescriptor.getNameAsString()));
        }

        if (!admin.isTableDisabled(TABLE))
        {
            admin.disableTable(TABLE);
        }

        admin.deleteTable(TABLE);
        Assert.assertFalse(admin.isTableAvailable(TABLE));
    }

    @Test
    public void testValidate() throws IOException
    {
        HTableDescriptor descriptor1 = new HTableDescriptor(TABLE);
        HColumnDescriptor columnDescriptor1 = new HColumnDescriptor(HBASE_ENTITY_SIMPLE);
        descriptor1.addFamily(columnDescriptor1);
        if (admin.isTableAvailable(TABLE))
        {
            admin.disableTable(TABLE);
            admin.deleteTable(TABLE);
        }
        admin.createTable(descriptor1);

        Assert.assertTrue(admin.isTableAvailable(TABLE));
        HTableDescriptor descriptor2 = admin.getTableDescriptor(TABLE.getBytes());
        Assert.assertNotNull(descriptor2.getFamilies());
        Assert.assertEquals(1, descriptor2.getFamilies().size());
        List<String> columns = new ArrayList<String>();
        columns.add(HBASE_ENTITY_SIMPLE);
        for (HColumnDescriptor columnDescriptor : descriptor2.getFamilies())
        {
            Assert.assertNotNull(columnDescriptor);
            Assert.assertNotNull(columnDescriptor.getNameAsString());
            Assert.assertTrue(columns.contains(columnDescriptor.getNameAsString()));
        }

        getEntityManagerFactory("validate");
        // schemaManager = new
        // HBaseSchemaManager(HBaseClientFactory.class.getName(), null);
        // schemaManager.exportSchema();
        if (!admin.isTableDisabled(TABLE))
        {
            admin.disableTable(TABLE);
        }
        admin.deleteTable(TABLE);
        Assert.assertFalse(admin.isTableAvailable(TABLE));
    }

    @Test
    public void testValidateInValid() throws IOException
    {
        try
        {
            HTableDescriptor descriptor1 = new HTableDescriptor(TABLE);
            HColumnDescriptor columnDescriptor1 = new HColumnDescriptor(HBASE_ENTITY_SIMPLE);
            descriptor1.addFamily(columnDescriptor1);
            if (admin.isTableAvailable(TABLE))
            {
                admin.disableTable(TABLE);
                admin.deleteTable(TABLE);
            }
            admin.createTable(descriptor1);
            Assert.assertTrue(admin.isTableAvailable(TABLE));
            HTableDescriptor descriptor2 = admin.getTableDescriptor(TABLE.getBytes());
            Assert.assertNotNull(descriptor2.getFamilies());
            Assert.assertEquals(1, descriptor2.getFamilies().size());
            for (HColumnDescriptor columnDescriptor : descriptor2.getFamilies())
            {
                Assert.assertNotNull(columnDescriptor);
                Assert.assertNotNull(columnDescriptor.getNameAsString());
                Assert.assertEquals(HBASE_ENTITY_SIMPLE, columnDescriptor.getNameAsString());
            }

            getEntityManagerFactory("validate");
            // schemaManager = new
            // HBaseSchemaManager(HBaseClientFactory.class.getName(), null);
            // schemaManager.exportSchema();
        }
        catch (SchemaGenerationException sgex)
        {
            List<String> errors = new ArrayList<String>();
            errors.add("column " + "AGE" + " does not exist in table " + TABLE + "");
            errors.add("column " + "PERSON_NAME" + " does not exist in table " + TABLE + "");
            Assert.assertTrue(errors.contains(sgex.getMessage()));

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

        props.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY, HBaseClientFactory.class.getName());
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "2181");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaHbaseTests");
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

        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        // appMetadata = null;
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(persistenceUnit);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put(persistenceUnit, null);
        metadata.put(persistenceUnit, puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(persistenceUnit);
        clazzToPu.put(HBaseEntitySimple.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        MetadataBuilder metadataBuilder = new MetadataBuilder(persistenceUnit, HBaseClient.class.getSimpleName(), null);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(HBaseEntitySimple.class, metadataBuilder.buildEntityMetadata(HBaseEntitySimple.class));

        appMetadata.getMetamodelMap().put(persistenceUnit, metaModel);

        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(persistenceUnit).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(persistenceUnit).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(persistenceUnit).getMappedSuperClassTypes());

//        KunderaMetadata.INSTANCE.addClientMetadata(persistenceUnit, clientMetadata);

        // String[] persistenceUnits = { persistenceUnit };
        new ClientFactoryConfiguraton(null, persistenceUnit).configure();
        configuration.configure();
        // new ClientFactoryConfiguraton(null, persistenceUnits).configure();

        return null;
    }
}
