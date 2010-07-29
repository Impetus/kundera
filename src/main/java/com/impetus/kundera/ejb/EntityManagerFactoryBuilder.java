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

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;
import javax.persistence.spi.PersistenceUnitTransactionType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Builds EmtityManagerFactory instances from classpath
 * 
 * @author animesh.kumar
 *
 */
public class EntityManagerFactoryBuilder {

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(EntityManagerFactoryBuilder.class);
    
    private static final String PROVIDER_IMPLEMENTATION_NAME = KunderaPersistence.class.getName();
    
    /**
     * Builds up EntityManagerFactory for a given persistenceUnitName and 
     * overriding properties.
     * 
     * @param persistenceUnitName
     * @param integration
     * @return
     */
    public EntityManagerFactory buildEntityManagerFactory (String persistenceUnitName, Map<Object, Object> override) {
    	PersistenceMetadata metadata = getPersistenceMetadata(persistenceUnitName);
    	
    	Properties props = new Properties();
    	// Override properties
    	Properties metadataProperties = metadata.getProps();
    	// Make sure, it's empty or Unmodifiable
    	override = override == null ? Collections.EMPTY_MAP : Collections.unmodifiableMap( override );
    	
    	// Take all from Metadata and override with supplied map 
    	for (Map.Entry<Object, Object> entry : metadataProperties.entrySet()) {
    		Object key = entry.getKey();
    		Object value = entry.getValue();
    		
    		if (override.containsKey(key)) {
    			value = override.get(key);
    		}
    		props.put(key, value);
    	}
    	
    	// Now take all the remaining ones from override
    	for (Map.Entry<Object, Object> entry : override.entrySet()) {
    		Object key = entry.getKey();
    		Object value = entry.getValue();

    		if (!props.containsKey(key)) {
    			props.put(key, value);
    		}
    	}
    	
    	log.info("Building EntityManagerFactory for name: " + metadata.getName() + ", and Properties:" + props);
    	return new EntityManagerFactoryImpl(metadata.getName(), props);
    }
    
	private PersistenceMetadata getPersistenceMetadata (String persistenceUnitName) {
		log.info( "Look up for persistence unit: " + persistenceUnitName );
		
		List<PersistenceMetadata> metadatas = findPersistenceMetadatas();
		
		// If there is just ONE persistenceUnit, then use this irrespective of the name
		if ( metadatas.size() == 1 ) {
			return metadatas.get(0);
		}
		
		// Since there is more persistenceUnits, you must provide a name to look up
		if ( isEmpty(persistenceUnitName) ) {
			throw new PersistenceException( "No name provided and several persistence units found" );
		}
		
		// Look for one that interests us
		for (PersistenceMetadata metadata : metadatas) {
			if (metadata.getName().equals(persistenceUnitName)) {
				return metadata;
			}
		}
		
		throw new PersistenceException("Could not find persistence unit in the classpath for name: " + persistenceUnitName);
	}
	
	private List<PersistenceMetadata> findPersistenceMetadatas () {
		try {
			Enumeration<URL> xmls = Thread.currentThread()
					.getContextClassLoader().getResources(
							"META-INF/persistence.xml");

			if (!xmls.hasMoreElements()) {
				log.info("Could not find any META-INF/persistence.xml " +
						"file in the classpath");
			}

			Set<String> persistenceUnitNames = new HashSet<String>();
			List<PersistenceMetadata> persistenceUnits = new ArrayList<PersistenceMetadata>();

			while (xmls.hasMoreElements()) {
				URL url = xmls.nextElement();
				log.trace("Analyse of persistence.xml: " + url);
				List<PersistenceMetadata> metadataFiles = PersistenceXmlLoader
						.findPersistenceUnits (url, PersistenceUnitTransactionType.RESOURCE_LOCAL);
				
				// Pick only those that have Kundera Provider
				for (PersistenceMetadata metadata : metadataFiles) {
					// check for provider
					if ( metadata.getProvider() == null || PROVIDER_IMPLEMENTATION_NAME.equalsIgnoreCase(
							metadata.getProvider()
					) ) {
						persistenceUnits.add(metadata);
					}
					
					// check for unique names
					if (persistenceUnitNames.contains(metadata.getName())) {
						throw new PersistenceException("Duplicate persistence-units for name: " + metadata.getName());
					}
					persistenceUnitNames.add(metadata.getName());
				}
			}
			
			return persistenceUnits;
		} catch (Exception e) {
			if (e instanceof PersistenceException) {
				throw (PersistenceException) e;
			} else {
				throw new PersistenceException(e);
			}
		}
	}
	
	// helper class
	private static boolean isEmpty (String str) {
		return null == str || str.isEmpty();
	}	
}
