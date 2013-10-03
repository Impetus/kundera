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
package com.impetus.client.persistence;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManager;

import junit.framework.Assert;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.thrift.ThriftClientFactory;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientFactoryConfiguraton;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.CoreMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.proxy.cglib.CglibLazyInitializerFactory;

/**
 * @author kuldeep.mishra
 * 
 *         The Class PersistWithNullField.
 */
public class NullableFieldAccessorTest
{

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");
        Cassandra.Client client = CassandraCli.getClient();
        client.set_keyspace("KunderaExamples");
        CfDef cf_def = new CfDef();
        cf_def.keyspace = "KunderaExamples";
        cf_def.name = "users";
        cf_def.setKey_validation_class("UTF8Type");
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("full_name".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        cf_def.addToColumn_metadata(columnDef2);
        ColumnDef columnDef3 = new ColumnDef(ByteBuffer.wrap("birth_date".getBytes()), "Int32Type");
        columnDef3.index_type = IndexType.KEYS;
        cf_def.addToColumn_metadata(columnDef3);
        ColumnDef columnDef4 = new ColumnDef(ByteBuffer.wrap("state".getBytes()), "UTF8Type");
        columnDef4.index_type = IndexType.KEYS;
        cf_def.addToColumn_metadata(columnDef4);
        client.system_add_column_family(cf_def);
    }

    /**
     * Test.
     */
    @Test
    public void test()
    {
        EntityManagerFactoryImpl emf = getEntityManagerFactory();
        EntityManager em = emf.createEntityManager();
        CassandraEntitySample entity = new CassandraEntitySample();
        entity.setKey("123");
        entity.setFull_name("kuldeep mishra");
        entity.setState("delhi");
        // birth_date is null

        em.persist(entity);

        CassandraEntitySample findEntity = em.find(CassandraEntitySample.class, 123);
        Assert.assertNotNull(findEntity);
        Assert.assertEquals("123", findEntity.getKey());
        Assert.assertEquals("kuldeep mishra", findEntity.getFull_name());
        Assert.assertEquals("delhi", findEntity.getState());
        Assert.assertNull(findEntity.getBirth_date());
        
        emf.close();

    }

    /**
     * Gets the entity manager factory.
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory()
    {
        Map<String, Object> props = new HashMap<String, Object>();
        String persistenceUnit = "cassandra";
        props.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY,
                "com.impetus.client.cassandra.pelops.PelopsClientFactory");
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaExamples");
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(persistenceUnit);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put("cassandra", puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(persistenceUnit);
        clazzToPu.put(CassandraEntitySample.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        MetadataBuilder metadataBuilder = new MetadataBuilder(persistenceUnit, ThriftClientFactory.class.getSimpleName(), null);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(CassandraEntitySample.class, metadataBuilder.buildEntityMetadata(CassandraEntitySample.class));
        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(persistenceUnit).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(persistenceUnit).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(persistenceUnit).getMappedSuperClassTypes());
        appMetadata.getMetamodelMap().put(persistenceUnit, metaModel);
        new ClientFactoryConfiguraton(null, persistenceUnit).configure();  
        
        CoreMetadata coreMetadata = new CoreMetadata();
        coreMetadata.setLazyInitializerFactory(new CglibLazyInitializerFactory());
        KunderaMetadata.INSTANCE.setCoreMetadata(coreMetadata);
        
        EntityManagerFactoryImpl emf = new EntityManagerFactoryImpl(persistenceUnit, props);
        return emf;
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
        CassandraCli.dropKeySpace("KunderaExamples");
    }
}
