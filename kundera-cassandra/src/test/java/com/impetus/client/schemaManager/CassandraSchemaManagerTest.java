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

package com.impetus.client.schemaManager;

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
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
/**
 * CassandraSchemaManagerTest class test the auto creation schema property in cassandra data store.  
 * 
 * @author Kuldeep.Kumar
 *
 */
public class CassandraSchemaManagerTest
{
    
    /** The configuration. */
    private SchemaConfiguration configuration;

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        configuration = new SchemaConfiguration("cassandra");
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test schema operation.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws TException the t exception
     * @throws InvalidRequestException the invalid request exception
     * @throws UnavailableException the unavailable exception
     * @throws TimedOutException the timed out exception
     * @throws SchemaDisagreementException the schema disagreement exception
     */
    @Test
    public void testSchemaOperation() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        getEntityManagerFactory();
        Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySimple", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySuper", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUni1To1", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUniMTo1", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUni1ToM", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUniMto1", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUni1ToM", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUni1To1", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonUni1To1PK", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressUni1To1PK", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBi1To1FK", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBi1To1PK", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBi1ToM", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityPersonBiMTo1", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBi1To1FK", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBi1To1PK", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBi1ToM", "KunderaCassandraExamples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntityAddressBiMTo1", "KunderaCassandraExamples"));
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
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY, "com.impetus.client.cassandra.pelops.PelopsClientFactory");
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaCassandraExamples");
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
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
        clazzToPu.put(CassandraEntitySimple.class.getName(), pus);
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

        EntityMetadata m = new EntityMetadata(CassandraEntitySimple.class);
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
        processor.process(CassandraEntitySimple.class, m);
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
        m10.setPersistenceUnit(persistenceUnit);
        m11.setPersistenceUnit(persistenceUnit);
        m12.setPersistenceUnit(persistenceUnit);
        m13.setPersistenceUnit(persistenceUnit);
        m14.setPersistenceUnit(persistenceUnit);
        m15.setPersistenceUnit(persistenceUnit);
        m16.setPersistenceUnit(persistenceUnit);
        m17.setPersistenceUnit(persistenceUnit);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(CassandraEntitySimple.class, m);
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

        appMetadata.getMetamodelMap().put(persistenceUnit, metaModel);

        configuration.configure();
        EntityManagerFactoryImpl impl = new EntityManagerFactoryImpl(puMetadata, props);
        return impl;
    }
}
