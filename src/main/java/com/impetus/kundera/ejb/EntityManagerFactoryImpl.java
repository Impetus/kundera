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
package com.impetus.kundera.ejb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;
import javax.persistence.spi.PersistenceUnitInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.CassandraClient;
import com.impetus.kundera.classreading.ClasspathReader;
import com.impetus.kundera.classreading.Reader;
import com.impetus.kundera.metadata.MetadataManager;

/**
 * The Class EntityManagerFactoryImpl.
 * 
 * @author animesh.kumar
 */
public class EntityManagerFactoryImpl implements EntityManagerFactory {

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(EntityManagerFactoryImpl.class);

    /** The Constant propsFileName. */
    private static final String propsFileName = "/kundera.properties";

    /** Whether or not the factory has been closed. */
    private boolean closed = false;

    /** Also the prefix that will be applied to each Domain. */
    private String persistenceUnitName;

    /** properties file values. */
    @SuppressWarnings("unchecked")
    private Map props;

    /** The sessionless. */
    private boolean sessionless;

    /** The metadata manager. */
    private MetadataManager metadataManager;

    private String[] nodes;
    
    private int port;
    
    private String keyspace;
    
    /** The client. */
    private CassandraClient client;

    /**
     * This one is generally called via the PersistenceProvider.
     * 
     * @param persistenceUnitInfo
     *            only using persistenceUnitName for now
     * @param props
     *            the props
     */
    public EntityManagerFactoryImpl(PersistenceUnitInfo persistenceUnitInfo, Map props) {
        this(persistenceUnitInfo != null ? persistenceUnitInfo.getPersistenceUnitName() : null, props);
    }

    /**
     * Use this if you want to construct this directly.
     * 
     * @param persistenceUnitName
     *            used to prefix the SimpleDB domains
     * @param props
     *            should have accessKey and secretKey
     */
    public EntityManagerFactoryImpl(String persistenceUnitName, Map props) {
        if (persistenceUnitName == null) {
            throw new IllegalArgumentException("Must have a persistenceUnitName!");
        }

        long start = System.currentTimeMillis();

        this.persistenceUnitName = persistenceUnitName;
        this.props = props;
        // if props is NULL or empty, look for kundera.properties and populate
        if (props == null || props.isEmpty()) {
            try {

                loadProperties(propsFileName);
            } catch (IOException e) {
                throw new PersistenceException(e);
            }
        }
        init();
        metadataManager = new MetadataManager(this);

        // scan classes for @Entity
        Reader reader = new ClasspathReader();
        reader.addValidAnnotations(Entity.class.getName());
        reader.addAnnotationDiscoveryListeners(metadataManager);
        reader.read();

        log.info("EntityManagerFactoryImpl loaded in " + (System.currentTimeMillis() - start) + "ms.");
    }

    /**
     * Load properties.
     * 
     * @param propsFileName
     *            the props file name
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void loadProperties(String propsFileName) throws IOException {
        Properties props_ = new Properties();
        InputStream stream = this.getClass().getResourceAsStream(propsFileName);
        if (stream == null) {
            throw new FileNotFoundException(propsFileName + " not found on classpath. Could not initialize Kundera.");
        }
        props_.load(stream);
        props = props_;
        stream.close();
    }

    /**
     * Inits the.
     */
    private void init() {
        nodes = ((String) props.get("kundera.nodes")).split(",");
        port = Integer.parseInt((String) props.get("kundera.port"));
        keyspace = (String) props.get("kundera.keyspace");

        String sessionless_ = (String) props.get("sessionless");
        if (sessionless_ == null) {
            sessionless = true;
        } else {
            sessionless = Boolean.parseBoolean(sessionless_);
        }

        String cassandraClient = (String) props.get("kundera.client");
        try {
            client = (CassandraClient) Class.forName(cassandraClient).newInstance();
            client.setContactNodes(nodes);
            client.setDefaultPort(port);
            // client.setKeySpace(keyspace);
            // connect to Cassandra DB
            client.connect();
        } catch (Exception e) {
            throw new IllegalArgumentException("Must define CassandraClient! " + e.getMessage());
        }
    }

    /**
     * Gets the metadata manager.
     * 
     * @return the metadataManager
     */
    public final MetadataManager getMetadataManager() {
        return metadataManager;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManagerFactory#close()
     */
    @Override
    public final void close() {
        closed = true;
        client.shutdown();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManagerFactory#createEntityManager()
     */
    @Override
    public final EntityManager createEntityManager() {
        return new EntityManagerImpl(this, client, sessionless);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.EntityManagerFactory#createEntityManager(java.util.Map)
     */
    @Override
    public final EntityManager createEntityManager(Map map) {
        return new EntityManagerImpl(this, client, sessionless);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManagerFactory#isOpen()
     */
    @Override
    public final boolean isOpen() {
        return !closed;
    }

    /**
     * Gets the persistence unit name.
     * 
     * @return the persistence unit name
     */
    public final String getPersistenceUnitName() {
        return persistenceUnitName;
    }

	public String[] getNodes() {
		return nodes;
	}

	public int getPort() {
		return port;
	}

	public String getKeyspace() {
		return keyspace;
	}
}
