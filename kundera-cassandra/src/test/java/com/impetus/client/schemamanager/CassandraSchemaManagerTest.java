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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;
import com.impetus.client.schemamanager.entites.CassandraEntityAddressBi1To1FK;
import com.impetus.client.schemamanager.entites.CassandraEntityAddressBi1To1PK;
import com.impetus.client.schemamanager.entites.CassandraEntityAddressBi1ToM;
import com.impetus.client.schemamanager.entites.CassandraEntityAddressBiMTo1;
import com.impetus.client.schemamanager.entites.CassandraEntityAddressUni1To1;
import com.impetus.client.schemamanager.entites.CassandraEntityAddressUni1To1PK;
import com.impetus.client.schemamanager.entites.CassandraEntityAddressUni1ToM;
import com.impetus.client.schemamanager.entites.CassandraEntityAddressUniMTo1;
import com.impetus.client.schemamanager.entites.CassandraEntityPersonBi1To1FK;
import com.impetus.client.schemamanager.entites.CassandraEntityPersonBi1To1PK;
import com.impetus.client.schemamanager.entites.CassandraEntityPersonBi1ToM;
import com.impetus.client.schemamanager.entites.CassandraEntityPersonBiMTo1;
import com.impetus.client.schemamanager.entites.CassandraEntityPersonUni1To1;
import com.impetus.client.schemamanager.entites.CassandraEntityPersonUni1To1PK;
import com.impetus.client.schemamanager.entites.CassandraEntityPersonUni1ToM;
import com.impetus.client.schemamanager.entites.CassandraEntityPersonUniMto1;
import com.impetus.client.schemamanager.entites.CassandraEntitySuper;
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
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * CassandraSchemaManagerTest class test the auto creation schema property in
 * cassandra data store.
 * 
 * @author Kuldeep.Kumar
 * 
 */
public class CassandraSchemaManagerTest
{
    private static final String _PU = "CassandraSchemaManager";

    private static final String _KEYSPACE = "CassandraSchemaManagerTest";

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
        CassandraCli.client.system_drop_keyspace(_KEYSPACE);
    }

    /**
     * Test schema operation.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws TException
     *             the t exception
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws UnavailableException
     *             the unavailable exception
     * @throws TimedOutException
     *             the timed out exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    @Test
    public void schemaOperation() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        getEntityManagerFactory();
        Assert.assertTrue(CassandraCli.keyspaceExist(_KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySuper", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUni1To1", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUniMTo1", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUni1ToM", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUniMto1", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUni1ToM", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUni1To1", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUni1To1PK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUni1To1PK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBi1To1FK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBi1To1PK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBi1ToM", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBiMTo1", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBi1To1FK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBi1To1PK", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBi1ToM", _KEYSPACE));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBiMTo1", _KEYSPACE));
    }

    /**
     * Gets the entity manager factory.
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory()
    {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(Constants.PERSISTENCE_UNIT_NAME, _PU);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY,
                "com.impetus.client.cassandra.pelops.PelopsClientFactory");
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, _KEYSPACE);
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(_PU);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put(_PU, puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(_PU);
        clazzToPu.put(CassandraEntitySuper.class.getName(), pus);
        clazzToPu.put(CassandraEntityAddressUni1To1.class.getName(), pus);
        clazzToPu.put(CassandraEntityAddressUni1ToM.class.getName(), pus);
        clazzToPu.put(CassandraEntityAddressUniMTo1.class.getName(), pus);
        clazzToPu.put(CassandraEntityPersonUniMto1.class.getName(), pus);
        clazzToPu.put(CassandraEntityPersonUni1To1.class.getName(), pus);
        clazzToPu.put(CassandraEntityPersonUni1ToM.class.getName(), pus);
        clazzToPu.put(CassandraEntityAddressUni1To1PK.class.getName(), pus);
        clazzToPu.put(CassandraEntityPersonUni1To1PK.class.getName(), pus);
        clazzToPu.put(CassandraEntityPersonBi1To1FK.class.getName(), pus);
        clazzToPu.put(CassandraEntityPersonBi1To1PK.class.getName(), pus);
        clazzToPu.put(CassandraEntityPersonBi1ToM.class.getName(), pus);
        clazzToPu.put(CassandraEntityPersonBiMTo1.class.getName(), pus);
        clazzToPu.put(CassandraEntityAddressBi1To1FK.class.getName(), pus);
        clazzToPu.put(CassandraEntityAddressBi1To1PK.class.getName(), pus);
        clazzToPu.put(CassandraEntityAddressBi1ToM.class.getName(), pus);
        clazzToPu.put(CassandraEntityAddressBiMTo1.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m1 = new EntityMetadata(CassandraEntitySuper.class);
        EntityMetadata m2 = new EntityMetadata(CassandraEntityAddressUni1To1.class);
        EntityMetadata m3 = new EntityMetadata(CassandraEntityAddressUni1ToM.class);
        EntityMetadata m4 = new EntityMetadata(CassandraEntityAddressUniMTo1.class);
        EntityMetadata m5 = new EntityMetadata(CassandraEntityPersonUniMto1.class);
        EntityMetadata m6 = new EntityMetadata(CassandraEntityPersonUni1To1.class);
        EntityMetadata m7 = new EntityMetadata(CassandraEntityPersonUni1ToM.class);
        EntityMetadata m8 = new EntityMetadata(CassandraEntityPersonUni1To1PK.class);
        EntityMetadata m9 = new EntityMetadata(CassandraEntityAddressUni1To1PK.class);
        EntityMetadata m10 = new EntityMetadata(CassandraEntityAddressBi1To1FK.class);
        EntityMetadata m11 = new EntityMetadata(CassandraEntityAddressBi1To1PK.class);
        EntityMetadata m12 = new EntityMetadata(CassandraEntityAddressBi1ToM.class);
        EntityMetadata m13 = new EntityMetadata(CassandraEntityAddressBiMTo1.class);
        EntityMetadata m14 = new EntityMetadata(CassandraEntityPersonBi1To1FK.class);
        EntityMetadata m15 = new EntityMetadata(CassandraEntityPersonBi1To1PK.class);
        EntityMetadata m16 = new EntityMetadata(CassandraEntityPersonBi1ToM.class);
        EntityMetadata m17 = new EntityMetadata(CassandraEntityPersonBiMTo1.class);

        TableProcessor processor = new TableProcessor();
        processor.process(CassandraEntitySuper.class, m1);
        processor.process(CassandraEntityAddressUni1To1.class, m2);
        processor.process(CassandraEntityAddressUni1ToM.class, m3);
        processor.process(CassandraEntityAddressUniMTo1.class, m4);
        processor.process(CassandraEntityPersonUniMto1.class, m5);
        processor.process(CassandraEntityPersonUni1To1.class, m6);
        processor.process(CassandraEntityPersonUni1ToM.class, m7);
        processor.process(CassandraEntityPersonUni1To1PK.class, m8);
        processor.process(CassandraEntityAddressUni1To1PK.class, m9);
        processor.process(CassandraEntityAddressBi1To1FK.class, m10);
        processor.process(CassandraEntityAddressBi1To1PK.class, m11);
        processor.process(CassandraEntityAddressBi1ToM.class, m12);
        processor.process(CassandraEntityAddressBiMTo1.class, m13);
        processor.process(CassandraEntityPersonBi1To1FK.class, m14);
        processor.process(CassandraEntityPersonBi1To1PK.class, m15);
        processor.process(CassandraEntityPersonBi1ToM.class, m16);
        processor.process(CassandraEntityPersonBiMTo1.class, m17);

        m1.setPersistenceUnit(_PU);
        m2.setPersistenceUnit(_PU);
        m3.setPersistenceUnit(_PU);
        m4.setPersistenceUnit(_PU);
        m5.setPersistenceUnit(_PU);
        m6.setPersistenceUnit(_PU);
        m7.setPersistenceUnit(_PU);
        m8.setPersistenceUnit(_PU);
        m9.setPersistenceUnit(_PU);
        m10.setPersistenceUnit(_PU);
        m11.setPersistenceUnit(_PU);
        m12.setPersistenceUnit(_PU);
        m13.setPersistenceUnit(_PU);
        m14.setPersistenceUnit(_PU);
        m15.setPersistenceUnit(_PU);
        m16.setPersistenceUnit(_PU);
        m17.setPersistenceUnit(_PU);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(CassandraEntitySuper.class, m1);
        metaModel.addEntityMetadata(CassandraEntityAddressUni1To1.class, m2);
        metaModel.addEntityMetadata(CassandraEntityAddressUni1ToM.class, m3);
        metaModel.addEntityMetadata(CassandraEntityAddressUniMTo1.class, m4);
        metaModel.addEntityMetadata(CassandraEntityPersonUniMto1.class, m5);
        metaModel.addEntityMetadata(CassandraEntityPersonUni1To1.class, m6);
        metaModel.addEntityMetadata(CassandraEntityPersonUni1ToM.class, m7);
        metaModel.addEntityMetadata(CassandraEntityPersonUni1To1PK.class, m8);
        metaModel.addEntityMetadata(CassandraEntityAddressUni1To1PK.class, m9);
        metaModel.addEntityMetadata(CassandraEntityAddressBi1To1FK.class, m10);
        metaModel.addEntityMetadata(CassandraEntityAddressBi1To1PK.class, m11);
        metaModel.addEntityMetadata(CassandraEntityAddressBi1ToM.class, m12);
        metaModel.addEntityMetadata(CassandraEntityAddressBiMTo1.class, m13);
        metaModel.addEntityMetadata(CassandraEntityPersonBi1To1FK.class, m14);
        metaModel.addEntityMetadata(CassandraEntityPersonBi1To1PK.class, m15);
        metaModel.addEntityMetadata(CassandraEntityPersonBi1ToM.class, m16);
        metaModel.addEntityMetadata(CassandraEntityPersonBiMTo1.class, m17);

        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(_PU).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(_PU).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(_PU).getMappedSuperClassTypes());

        appMetadata.getMetamodelMap().put(_PU, metaModel);

        new ClientFactoryConfiguraton(_PU).configure();
        new SchemaConfiguration(_PU).configure();
        // EntityManagerFactoryImpl impl = new
        // EntityManagerFactoryImpl(puMetadata, props);
        return null;
    }
}
