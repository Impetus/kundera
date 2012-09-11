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
package com.impetus.client.hbase.config;

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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.HBaseClientFactory;
import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientFactoryConfiguraton;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.ClientMetadata;
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
public class SchemaGenerationUsingPropertyTest
{
    private String persistenceUnit = "UsePropertyInHbase";

    /** The configuration. */
    private static SchemaConfiguration configuration;

    private final boolean useLucene = false;

    private static HBaseAdmin admin;

    private static HBaseCli cli;

    private HBaseColumnFamilyProperties hcfp = null;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        cli = new HBaseCli();
        cli.startCluster();
        if (admin == null)
        {
            admin = cli.utility.getHBaseAdmin();
        }
        configuration = new SchemaConfiguration(persistenceUnit);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        cli.stopCluster();
    }

    @Test
    public void test() throws IOException
    {
        getEntityManagerFactory("create");
        hcfp = HBasePropertyReader.hsmd.getColumnFamilyProperties().get("HBaseEntity");
        Assert.assertTrue(admin.isTableAvailable("HBaseEntity"));
        HTableDescriptor descriptor = admin.getTableDescriptor("HBaseEntity".getBytes());
        Assert.assertNotNull(descriptor.getFamilies());
        Assert.assertEquals(2, descriptor.getFamilies().size());
        List<String> columns = new ArrayList<String>();
        columns.add("AGE");
        columns.add("NAME");
        for (HColumnDescriptor columnDescriptor : descriptor.getFamilies())
        {
            Assert.assertNotNull(columnDescriptor);
            Assert.assertNotNull(columnDescriptor.getNameAsString());
            Assert.assertTrue(columns.contains(columnDescriptor.getNameAsString()));
            Assert.assertEquals(hcfp.getMaxVersion(), columnDescriptor.getMaxVersions());
            Assert.assertEquals(hcfp.getMinVersion(), columnDescriptor.getMinVersions());
            Assert.assertEquals(hcfp.getTtl(), columnDescriptor.getTimeToLive());
            Assert.assertEquals(hcfp.getAlgorithm(), columnDescriptor.getCompactionCompressionType());
            Assert.assertEquals(hcfp.getAlgorithm(), columnDescriptor.getCompressionType());
        }

        admin.disableTable("HBaseEntity");
        admin.deleteTable("HBaseEntity");
        Assert.assertFalse(admin.isTableAvailable("HBaseEntity"));
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
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaHbase");
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, property);
        props.put(PersistenceProperties.KUNDERA_CLIENT_PROPERTY, "kundera-hbase.properties");
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
        metadata.put(persistenceUnit, puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(persistenceUnit);
        clazzToPu.put(HBaseEntity.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(HBaseEntity.class);

        TableProcessor processor = new TableProcessor();
        processor.process(HBaseEntity.class, m);

        m.setPersistenceUnit(persistenceUnit);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(HBaseEntity.class, m);

        appMetadata.getMetamodelMap().put(persistenceUnit, metaModel);
        
        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(persistenceUnit).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(persistenceUnit).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(persistenceUnit).getMappedSuperClassTypes());

        KunderaMetadata.INSTANCE.addClientMetadata(persistenceUnit, clientMetadata);

        String[] persistenceUnits = { persistenceUnit };
        new ClientFactoryConfiguraton(persistenceUnits).configure();
        configuration.configure();
        // EntityManagerFactoryImpl impl = new
        // EntityManagerFactoryImpl(puMetadata, props);
        return null;
    }
}
