/*
 * Copyright (c) 2010-2011, Animesh Kumar
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
 * @author animesh.kumar
 *
 */
public class EntityManagerFactoryImpl implements EntityManagerFactory {

	/** the log used by this class. */
	private static Log log = LogFactory.getLog(EntityManagerFactoryImpl.class);

	static final String propsFileName = "/kundera.properties";
	
    /**
     * Whether or not the factory has been closed
     */
    private boolean closed = false;
    /**
     * Also the prefix that will be applied to each Domain
     */
    private String persistenceUnitName;
    /**
     * properties file values
     */
    @SuppressWarnings("unchecked")
	public Map props;    
    
    private boolean sessionless;
    private String[] cassandraNodes;
    private int cassandraPort;
    private String cassandraKeyspace;
    
    private MetadataManager metadataManager;
    
    public CassandraClient client;
    /**
     * This one is generally called via the PersistenceProvider.
     *
     * @param persistenceUnitInfo only using persistenceUnitName for now
     * @param props
     */
    public EntityManagerFactoryImpl(PersistenceUnitInfo persistenceUnitInfo, Map props) {
        this(persistenceUnitInfo != null ? persistenceUnitInfo.getPersistenceUnitName() : null, props);
    }

    /**
     * Use this if you want to construct this directly.
     *
     * @param persistenceUnitName used to prefix the SimpleDB domains
     * @param props               should have accessKey and secretKey
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
    
    private void init() {
        cassandraNodes = ((String) props.get("kundera.nodes")).split(",");
        cassandraPort = Integer.parseInt((String) props.get("kundera.port"));
        cassandraKeyspace = (String) props.get("kundera.keyspace");
        
        String sessionless_ = (String) props.get("sessionless");
        if (sessionless_ == null) {
            sessionless = true;
        } else {
            sessionless = Boolean.parseBoolean(sessionless_);
        }
        
        String cassandraClient = (String) props.get("kundera.client");
        try {
			client = (CassandraClient) Class.forName(cassandraClient).newInstance();
	        client.setContactNodes(cassandraNodes);
	        client.setDefaultPort(cassandraPort);
	        client.setKeySpace(cassandraKeyspace);
	        // connect to Cassandra DB
	        client.connect();
		} catch (Exception e) {
			throw new IllegalArgumentException("Must define CassandraClient! " + e.getMessage());
		}
    }

	/**
	 * @return the metadataManager
	 */
	public MetadataManager getMetadataManager() {
		return metadataManager;
	}

	@Override
	public void close() {
        closed = true;
        client.shutdown();
	}

	@Override
	public EntityManager createEntityManager() {
		return new EntityManagerImpl(this, client, sessionless);
	}

	@Override
	public EntityManager createEntityManager(Map map) {
		return new EntityManagerImpl(this, client, sessionless);
	}

	@Override
	public boolean isOpen() {
        return !closed;
	}

	public String getPersistenceUnitName() {
		return persistenceUnitName;
	}
}
