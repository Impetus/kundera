/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManager;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerImpl;
import com.impetus.kundera.query.QueryImpl;

/**
 * Junit test case for NativeQuery support.
 * 
 * @author vivek.mishra
 *
 */
public class NativeQueryTest
{

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testCreateNativeQuery()
    {
        Map<String, Object> props = new HashMap<String, Object>();
        String persistneceUnit = "cassandra";
        props.put(Constants.PERSISTENCE_UNIT_NAME, persistneceUnit );
        props.put(PersistenceProperties.KUNDERA_CLIENT, "pelops");
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "test");
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(persistneceUnit);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put("cassandra", puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);
        
        Map<String, List<String>> clazzToPu= new HashMap<String, List<String>>();
        
        List<String> pus = new ArrayList<String>();
        pus.add(persistneceUnit);
        clazzToPu.put(CassandraEntitySample.class.getName(), pus);
        
        appMetadata.setClazzToPuMap(clazzToPu);
        
        EntityManagerFactoryImpl emf = new EntityManagerFactoryImpl(persistneceUnit, props);
        EntityManager em = new EntityManagerImpl(emf);
        String nativeSql = "Select * from Cassandra c";
        QueryImpl q = (QueryImpl) em.createNativeQuery(nativeSql, CassandraEntitySample.class);
        Assert.assertEquals(nativeSql, q.getJPAQuery());
    }
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

}
