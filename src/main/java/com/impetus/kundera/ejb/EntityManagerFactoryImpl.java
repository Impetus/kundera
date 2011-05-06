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

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceUnitInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.cache.Cache;
import com.impetus.kundera.cache.CacheException;
import com.impetus.kundera.cache.CacheProvider;
import com.impetus.kundera.classreading.ClasspathReader;
import com.impetus.kundera.classreading.Reader;
import com.impetus.kundera.metadata.MetadataManager;
import com.impetus.kundera.proxy.EnhancedEntity;
import com.impetus.kundera.proxy.EntityEnhancerFactory;
import com.impetus.kundera.proxy.KunderaProxy;
import com.impetus.kundera.proxy.LazyInitializerFactory;
import com.impetus.kundera.proxy.cglib.CglibEntityEnhancerFactory;
import com.impetus.kundera.proxy.cglib.CglibLazyInitializerFactory;

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

    /** The nodes. */
    private String[] nodes;

    /** The port. */
    private Integer port;

    /** The keyspace. */
    private String keyspace;


    /** The Cache provider. */
    private CacheProvider cacheProvider;
    
    private String cacheProviderClassName;
    
    /** The enhanced proxy factory. */
    private EntityEnhancerFactory enhancedProxyFactory;
    
    /** The lazy initializer factory. */
    private LazyInitializerFactory lazyInitializerFactory;
    
    /**
     * A convenience constructor.
     * 
     * @param persistenceUnitName
     *            used to prefix the Cassandra domains
     */
    public EntityManagerFactoryImpl(String persistenceUnitName) {
        this(persistenceUnitName, null);
    }
    
//    /**
//     * Parameterize constructor to load configuration for no sql databases other than Cassandra.  
//     * @param persistenceUnitName
//     * @param isCassandra
//     */
//    public EntityManagerFactoryImpl(String persistenceUnitName, boolean isCassandra){
//          onLoad(persistenceUnitName);
//        if(isCassandra) {
//            loadCassandraProps();
//            initCassandraClient();
//        } else {
//        	try{
//        		loadProperties(propsFileName);
//        	}catch(IOException ioex) {
//        		throw new PersistenceException(ioex.getMessage());
//        	}
//        	loadHBase();
//        }
//        
//    	// Second level cache
//		initSecondLevelCache();
//    }

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
     *            used to prefix the Cassandra domains
     * @param props
     *            should have accessKey and secretKey
     */
    public EntityManagerFactoryImpl(String persistenceUnitName, Map props) {
       onLoad(persistenceUnitName);
      this.props = props;
//        // if props is NULL or empty, look for kundera.properties and populate
//        if (props == null || props.isEmpty()) {
//            loadCassandraProps();
//            initCassandraClient();
//        }
//        //configure Cassandra client.
    	// Second level cache
//		initSecondLevelCache();
    }


//    /**
//     * Loads cassandra properties.
//     */
//	private void loadCassandraProps() {
//		try {
//			log.debug("Trying to load Kundera Properties from " + propsFileName);
//		    loadProperties(propsFileName);
//		} catch (IOException e) {
//		    throw new PersistenceException(e);
//		}
//	}
	
//	/**
//	 * Loads HBase properties.
//	 */
//	private void loadHBase() {
//		client = new HBaseClient();
//		client.setContactNodes("localhost");
//		client.setDefaultPort(6000);
//		client.connect();
//	}

    /**
     * Method to instantiate persistence entities and metadata.
     * @param persistenceUnitName
     */
	private void onLoad(String persistenceUnitName) {
		if (persistenceUnitName == null) {
            throw new IllegalArgumentException("Must have a persistenceUnitName!");
        }
        long start = System.currentTimeMillis();

        this.persistenceUnitName = persistenceUnitName;
        metadataManager = new MetadataManager(this);

        // scan classes for @Entity
        Reader reader = new ClasspathReader();
        reader.addValidAnnotations(Entity.class.getName());
        reader.addAnnotationDiscoveryListeners(metadataManager);
        reader.read();

        metadataManager.build();
        
        enhancedProxyFactory = new CglibEntityEnhancerFactory();
        lazyInitializerFactory = new CglibLazyInitializerFactory();
        
        log.info("EntityManagerFactoryImpl loaded in " + (System.currentTimeMillis() - start) + "ms.");
	}

    
//    /**
//     * Load properties.
//     * 
//     * @param propsFileName
//     *            the props file name
//     * 
//     * @throws IOException
//     *             Signals that an I/O exception has occurred.
//     */
//    private void loadProperties(String propsFileName) throws IOException {
//        Properties props_ = new Properties();
//        InputStream stream = this.getClass().getResourceAsStream(propsFileName);
//        if (stream == null) {
//            throw new FileNotFoundException(propsFileName + " not found on classpath. Could not initialize Kundera.");
//        }
//        props_.load(stream);
//        props = props_;
//        stream.close();
//    }

//    /**
//     * Inits the.
//     */
//    private void initCassandraClient() {
//    	// Look for kundera.nodes
//    	try {
//    		String kunderaNodes = (String)props.get("kundera.nodes");
//    		if (null == kunderaNodes || kunderaNodes.isEmpty()) {
//    			throw new IllegalArgumentException();
//    		}
//    		nodes = kunderaNodes.split(",");
//    	} catch (Exception e) {
//    		throw new IllegalArgumentException("Mandatory property missing 'kundera.nodes'");
//    	}
//        
//    	// kundera.port
//    	String kunderaPort = (String) props.get("kundera.port");
//		if (null == kunderaPort || kunderaPort.isEmpty()) {
//			throw new IllegalArgumentException("Mandatory property missing 'kundera.port'");
//		}
//    	try {
//    		port = Integer.parseInt(kunderaPort);
//    	} catch (Exception e) {
//    		throw new IllegalArgumentException("Invalid value for property 'kundera.port': " + kunderaPort + ". (Should it be 9160?)");
//    	}
//        
//    	// kundera.keyspace
//    	keyspace = (String) props.get("kundera.keyspace");
//		if (null == keyspace || keyspace.isEmpty()) {
//			throw new IllegalArgumentException("Mandatory property missing 'kundera.keyspace'");
//		}
//        
//        // kundera.client
//        String cassandraClient = (String) props.get("kundera.client");
//		if (null == cassandraClient || cassandraClient.isEmpty()) {
//			throw new IllegalArgumentException("Mandatory property missing 'kundera.client'");
//		}
//	
//		
//		// Instantiate the client
//        try {
//    		if ( cassandraClient.endsWith( ".class" ) ) {
//    			cassandraClient = cassandraClient.substring( 0, cassandraClient.length() - 6 );
//    		}
//    		
//            client = (CassandraClient) Class.forName(cassandraClient).newInstance();
//            client.setContactNodes(nodes);
//            client.setDefaultPort(port);
//        } catch (Exception e) {
//            throw new IllegalArgumentException("Invalid value for property 'kundera.client': " + cassandraClient + ". (Should it be com.impetus.kundera.client.PelopsClient?");
//        }
//        
//        log.info("Connecting to Cassandra... (nodes:" + Arrays.asList(nodes) + ", port:" + port + ", keyspace:" + keyspace + ")");
//        
//        // connect to Cassandra DB
//        client.connect();
//    }

//    /**
//	 * Inits the second level cache.
//	 */
//    @SuppressWarnings("unchecked")
//	private void initSecondLevelCache() {
////    	String cacheProviderClassName = (String) props.get("kundera.cache.provider_class");
//        if (cacheProviderClassName != null) {
//            try {
//                Class<CacheProvider> cacheProviderClass = (Class<CacheProvider>) Class.forName(cacheProviderClassName);
//                cacheProvider = cacheProviderClass.newInstance();
//                cacheProvider.init(props);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
//        if (cacheProvider == null) {
//        	cacheProvider = new NonOperationalCacheProvider();
//        }
//        log.info("Initialized second-level cache. Provider: " + cacheProvider.getClass());
//    }
    
    /**
	 * Gets the cache.
	 * 
	 * @param entity
	 *            the entity
	 * @return the cache
	 */
    public Cache getCache(Class<?> entity) {
        try {
            String cacheName = metadataManager.getEntityMetadata(entity).getEntityClazz().getName();
            return cacheProvider.createCache(cacheName);
        } catch (CacheException e) {
            throw new RuntimeException(e);
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

    /* @see javax.persistence.EntityManagerFactory#close() */
    @Override
    public final void close() {
        closed = true;
//        client.shutdown();
        cacheProvider.shutdown();
    }

    /* @see javax.persistence.EntityManagerFactory#createEntityManager() */
    @Override
    public final EntityManager createEntityManager() {
        return new EntityManagerImpl(this);
    }

    /* @see javax.persistence.EntityManagerFactory#createEntityManager(java.util.Map) */
    @Override
    public final EntityManager createEntityManager(Map map) {
        return new EntityManagerImpl(this);
    }

    /**
	 * Gets the enhanced entity.
	 * 
	 * @param entity
	 *            the entity
	 * @param id
	 *            the id
	 * @param foreignKeyMap
	 *            the foreign key map
	 * @return the enhanced entity
	 */
    public EnhancedEntity getEnhancedEntity (Object entity, String id, 
			Map<String, Set<String>> foreignKeyMap) {
    	return enhancedProxyFactory.getProxy(entity, id, foreignKeyMap);
    }

    /**
	 * Gets the lazy entity.
	 * 
	 * @param entityName
	 *            the entity name
	 * @param persistentClass
	 *            the persistent class
	 * @param getIdentifierMethod
	 *            the get identifier method
	 * @param setIdentifierMethod
	 *            the set identifier method
	 * @param id
	 *            the id
	 * @param em
	 *            the em
	 * @return the lazy entity
	 */
    public KunderaProxy getLazyEntity (String entityName,
			Class<?> persistentClass,
			Method getIdentifierMethod, Method setIdentifierMethod, String id,
			EntityManagerImpl em) {
    	return lazyInitializerFactory.getProxy(
    			entityName, 
    			persistentClass,  
    			getIdentifierMethod, 
    			setIdentifierMethod, 
				id, 
				em);
    }
    
    /* @see javax.persistence.EntityManagerFactory#isOpen() */
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

    /**
     * Gets the nodes.
     * 
     * @return the nodes
     */
    public String[] getNodes() {
        return nodes;
    }

    /**
     * Gets the port.
     * 
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets the keyspace.
     * 
     * @return the keyspace
     */
    public String getKeyspace() {
        return keyspace;
    }
}
