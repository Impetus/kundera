package com.impetus.kundera.loader;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;
import javax.persistence.spi.PersistenceUnitTransactionType;

import com.impetus.kundera.Client;
import com.impetus.kundera.cache.CacheProvider;
import com.impetus.kundera.cache.NonOperationalCacheProvider;
import com.impetus.kundera.ejb.EntityManagerFactoryImpl;
import com.impetus.kundera.ejb.PersistenceMetadata;
import com.impetus.kundera.ejb.PersistenceXmlLoader;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Configuration loader.
 * @author impetus
 *
 */
public class Configuration {

	private Map<ClientIdentifier, EntityManagerFactory> emfMap = new HashMap<ClientIdentifier, EntityManagerFactory>();
	private Map<ClientIdentifier, EntityManager> emMap = new HashMap<ClientIdentifier, EntityManager>();
	private String node;
	private String  port;
	private String keyspace;
	private ClientIdentifier identifier;
	
	/**
	 * 
	 * @param persistenceUnits
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public EntityManager getEntityManager(String persistenceUnit) {
		EntityManager em;
//	  for(String persistenceUnit:persistenceUnits) {
		 EntityManagerFactory emf =  (EntityManagerFactoryImpl)Persistence.createEntityManagerFactory(persistenceUnit);
			 try {
				Map propMap = (Map) PropertyAccessorHelper.getObject(emf,emf.getClass().getDeclaredField("props"));
				Properties props = new Properties();
				props.putAll(propMap);
				String client = props.getProperty("kundera.client");
				node = props.getProperty("kundera.nodes");
				port = props.getProperty("kundera.port");
				keyspace = props.getProperty("kundera.keyspace");
				String resourceName = "net.sf.ehcache.configurationResourceName";
				ClientType clientType = ClientType.getValue(client.toUpperCase());
				createIdentifier(clientType,persistenceUnit);
				setField(emf, emf.getClass().getDeclaredField("cacheProvider"), initSecondLevelCache(props.getProperty("kundera.cache.provider_class"),resourceName));
				emfMap.put(identifier, emf);
				 em = emf.createEntityManager();
				setClient(em, clientType, persistenceUnit);
				emMap.put(identifier, em);				
				System.out.println("Kundera Client is: " + props.getProperty("kundera.client"));
			} catch (SecurityException e) {
				throw new PersistenceException(e.getMessage());
			} catch (PropertyAccessException e) {
				throw new PersistenceException(e.getMessage());
			} catch (NoSuchFieldException e) {
				throw new PersistenceException(e.getMessage());
			}
			return em;
	  }
	
	
	
	/**
	 * Initialises 
	 * @param url
	 */
	public void init(URL url) {
		try {
			List<PersistenceMetadata> metadataCol = PersistenceXmlLoader.findPersistenceUnits(url, PersistenceUnitTransactionType.JTA);
			for(PersistenceMetadata metadata: metadataCol) {
				Properties props = metadata.getProps();
				String client = props.getProperty("kundera.client");
				node = props.getProperty("kundera.nodes");
				port = props.getProperty("kundera.port");
				keyspace = props.getProperty("kundera.keyspace");
				String resourceName = "net.sf.ehcache.configurationResourceName";
				ClientType clientType = ClientType.getValue(client.toUpperCase());
				createIdentifier(clientType,metadata.getName());
				if(!emfMap.containsKey(identifier)){
					EntityManagerFactory emf = Persistence.createEntityManagerFactory(metadata.getName());
					setField(emf, emf.getClass().getDeclaredField("cacheProvider"), initSecondLevelCache(props.getProperty("kundera.cache.provider_class"),resourceName));
					emfMap.put(identifier, emf);
					EntityManager em = emf.createEntityManager();
					setClient(em, clientType, metadata.getName());
					emMap.put(identifier, em);
					System.out.println((emf.getClass().getDeclaredField("cacheProvider")));
				}
			}
		} catch (Exception e) {
			throw new PersistenceException(e.getMessage());
		}
	}
	
	/**
	 * Returns entityManager.
	 * @param clientType client type.
	 * @return em entityManager.
	 */
	public EntityManager getEntityManager(ClientType clientType) {
		return emMap.get(clientType);
	}
	
	/**
	 *  Invoked on end of application.
	 */
	public void destroy() {
		for(EntityManager em : emMap.values()) {
			if(em.isOpen()) {
			em.flush();
			em.clear();
			em.close();
			em=null;
			}
		}
		for(EntityManagerFactory emf: emfMap.values()) {
			emf.close();
			emf=null;
		}
	}
	
	/**
	 * Set client to entity manager.
	 * @param em
	 * @param clientType
	 */
	private void setClient(EntityManager em,ClientType clientType, String persistenceUnit) {
		try {
		  setField(em,em.getClass().getDeclaredField("client"), getClient(clientType, persistenceUnit));
		} catch (NoSuchFieldException e) {
			throw new PersistenceException(e.getMessage());
	}
 }

	/**
	 * 
	 * @param obj
	 * @param f
	 * @param value
	 */
	private void setField(Object obj, Field f, Object value) {
		try {
			PropertyAccessorHelper.set(obj,f, value);
		} catch (SecurityException e) {
			throw new PersistenceException(e.getMessage());		
		} catch (PropertyAccessException e) {
			throw new PersistenceException(e.getMessage());
		} 
	}

	/**
	 * 
	 * @param clientType
	 * @return
	 */
	private Client getClient(ClientType clientType, String persistenceUnit) {
//		ClientIdentifier identifier = getIdentifier(clientType, persistenceUnit);
		Client client = ClientResolver.getClient(identifier);
    	client.connect();
		return client;
	}

	private void  createIdentifier(ClientType clientType, String persistenceUnit) {
		identifier = new ClientIdentifier(new String[]{node}, Integer.valueOf(port), keyspace, clientType, persistenceUnit);
	}
	
	
	
	/*
	*//**
	/**
	 * Loads kundera properties.
	 *//*
	private void loadKunderaProperties(Properties props) {
		// Look for kundera.nodes
    	try {
    		String kunderaNodes = (String)props.getProperty("kundera.nodes");
    		if (null == kunderaNodes || kunderaNodes.isEmpty()) {
    			throw new IllegalArgumentException();
    		}
    		nodes = kunderaNodes.split(",");
    	} catch (Exception e) {
    		throw new IllegalArgumentException("Mandatory property missing 'kundera.nodes'");
    	}
        
    	// kundera.port
    	String kunderaPort = props.getProperty("kundera.port");
		if (null == kunderaPort || kunderaPort.isEmpty()) {
			throw new IllegalArgumentException("Mandatory property missing 'kundera.port'");
		}
    	try {
    		port = Integer.parseInt(kunderaPort);
    	} catch (Exception e) {
    		throw new IllegalArgumentException("Invalid value for property 'kundera.port': " + kunderaPort + ". (Should it be 9160?)");
    	}
        
    	// kundera.keyspace
    	keyspace = props.getProperty("kundera.keyspace");
		if (null == keyspace || keyspace.isEmpty()) {
			throw new IllegalArgumentException("Mandatory property missing 'kundera.keyspace'");
		}
        
        // kundera.client
        String client = props.getProperty("kundera.client");
		if (null == client|| client.isEmpty()) {
			throw new IllegalArgumentException("Mandatory property missing 'kundera.client'");
		}
	
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

	}*/

	@SuppressWarnings("unchecked")
	private CacheProvider initSecondLevelCache(String cacheProviderClassName, String classResourceName) {
//    	String cacheProviderClassName = (String) props.get("kundera.cache.provider_class");
		CacheProvider cacheProvider=null;
        if (cacheProviderClassName != null) {
            try {
                Class<CacheProvider> cacheProviderClass = (Class<CacheProvider>) Class.forName(cacheProviderClassName);
                cacheProvider = cacheProviderClass.newInstance();
                cacheProvider.init(classResourceName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (cacheProvider == null) {
        	cacheProvider = new NonOperationalCacheProvider();
        }
   return cacheProvider;
    }}
