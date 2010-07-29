/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.junit;

import java.net.URL;

import javax.persistence.Persistence;

import junit.framework.TestCase;

import org.apache.cassandra.service.EmbeddedCassandraService;

import com.impetus.kundera.ejb.EntityManagerFactoryImpl;

/**
 * @author animesh.kumar
 *
 */
public class PersistenceUnitTest extends TestCase {

    /** The embedded server cassandra. */
    private static EmbeddedCassandraService cassandra;

    
    public void startCassandraServer () throws Exception {
        URL configURL = TestKundera.class.getClassLoader().getResource("storage-conf.xml");
        try {
            String storageConfigPath = configURL.getFile().substring(1).substring(0, configURL.getFile().lastIndexOf("/"));
            System.setProperty("storage-config", storageConfigPath);
        } catch (Exception e) {
            fail("Could not find storage-config.xml sfile");
        }
        cassandra = new EmbeddedCassandraService();
        cassandra.init();
        Thread t = new Thread(cassandra);
        t.setDaemon(true);
        t.start();   	
    }
    
    public void setUp() throws Exception {
    	startCassandraServer();
    }
    
    public void testPersistenceUnit() {
        EntityManagerFactoryImpl emf = (EntityManagerFactoryImpl)Persistence.createEntityManagerFactory("test-unit-2");
        assertEquals(emf.getKeyspace(), "Blog");
        assertEquals(emf.getPersistenceUnitName(), "test-unit-2");
        assertEquals(emf.getPort(), 9165);
        emf.close();
    }
}
