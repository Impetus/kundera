/*
 * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.client;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Client;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.loader.DBType;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * CLient class for MongoDB database
 * @author amresh.singh
 */
public class MongoDBClient implements Client {
	String contactNode;
	String defaultPort;	
	private boolean isConnected;
	 
	private EntityManager em;
	
	Mongo mongo;
	DB mongoDb;

	private static Log log = LogFactory.getLog(MongoDBClient.class);


	@Override
	public void writeColumns(String dbName, String collectionName, String key,
			List<Column> columns, EnhancedEntity e) throws Exception {
		log.debug("Persisting data into " + dbName + "." + collectionName + " for " + key);
		DBCollection dbCollection = mongoDb.getCollection(collectionName);	
		
		BasicDBObject document = getDocumentObject(columns, e);	
		dbCollection.insert(document);	
	}	

	
	@Override
	public <E> E loadColumns(EntityManagerImpl em, Class<E> clazz,
			String dbName, String collectionName, String key, EntityMetadata m)
			throws Exception {		
		log.debug("Fetching data from " + collectionName + " for PK " + key);
		DBCollection dbCollection = mongoDb.getCollection(collectionName);
		
		BasicDBObject query = new BasicDBObject();
        query.put(m.getIdColumn().getName(), key);

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;
        
        if(cursor.hasNext()) {
            fetchedDocument = cursor.next();
        } else {
        	return null;
        }
        
        E entity = clazz.newInstance();
        List<Column> columns = m.getColumnsAsList();
        for(Column column : columns) {
        	PropertyAccessorHelper.set(entity, column.getField(), fetchedDocument.get(column.getName()));
        }
        return entity;
	}

	
	@Override
	public <E> List<E> loadColumns(EntityManagerImpl em, Class<E> clazz,
			String dbName, String collectionName, EntityMetadata m,
			String... keys) throws Exception {
		log.debug("Fetching data from " + collectionName + " for Keys " + keys);
		
		DBCollection dbCollection = mongoDb.getCollection(collectionName);
		
		BasicDBObject query = new BasicDBObject();
        query.put(m.getIdColumn().getName(), new BasicDBObject("$in", keys));			

        DBCursor cursor = dbCollection.find(query);		
        
        List<E> entities = new ArrayList<E>();
        List<Column> columns = m.getColumnsAsList();
        while(cursor.hasNext()) {
			DBObject fetchedDocument = cursor.next();
			E entity = clazz.newInstance();
			
			for(Column column : columns) {
	        	PropertyAccessorHelper.set(entity, column.getField(), fetchedDocument.get(column.getName()));
	        }
			entities.add(entity);			
		}       
		return entities;
	}
	
	@Override
	public void delete(String idColumnName, String collectionName, String rowId)
			throws Exception {		
		DBCollection dbCollection = mongoDb.getCollection(collectionName);		
		
		//Find the DBObject to remove first
		BasicDBObject query = new BasicDBObject();
        query.put(idColumnName, rowId);

        DBCursor cursor = dbCollection.find(query);
        DBObject documentToRemove = null;
        
        if(cursor.hasNext()) {
            documentToRemove = cursor.next();
        } else {
        	throw new PersistenceException("Can't remove Row# " + rowId + " for "
        			+ collectionName + " because record doesn't exist.");
        }
		
		dbCollection.remove(documentToRemove);				
	}
	
	@Override
	public void connect() {
		if(!isConnected) {
			log.info(">>> Connecting to MONGODB at " + contactNode + " on port " + defaultPort);
			try {
				mongo = new Mongo(contactNode, Integer.parseInt(defaultPort));
				mongoDb = mongo.getDB("mongotest");
				isConnected=true;
				log.info("CONNECTED to MONGODB at " + contactNode + " on port " + defaultPort);
			} catch (NumberFormatException e) {
				log.error("Invalid format for MONGODB port, Unale to connect!" + "; Details:" + e.getMessage());
			} catch (UnknownHostException e) {
				log.error("Unable to connect to MONGODB at host " + contactNode + "; Details:" + e.getMessage());
			} catch (MongoException e) {
				log.error("Unable to connect to MONGODB; Details:" + e.getMessage());
			}			
		}
	}
	
	@Override
	public void shutdown() {
		if(isConnected && mongo != null) {
			log.info("Closing connection to MONGODB at " + contactNode + " on port " + defaultPort);
			mongo.close();
		} else {
			log.warn("Can't close connection to MONGODB, it was already disconnected");
		}
	}
	
	private BasicDBObject getDocumentObject(List<Column> columns, EnhancedEntity e) {
		Object entity = e.getEntity();	
		BasicDBObject dbObj = new BasicDBObject();	
		
		for(Column column : columns) {
			try {
				dbObj.put(column.getName(), PropertyAccessorHelper.getString(entity, column.getField()));
			} catch (PropertyAccessException e1) {				
				log.error("Can't access property " + column.getField().getName());
			}
		}		
		return dbObj;
	}	
	

	
	@Override
	public DBType getType() {		
		return DBType.MONGODB;
	}
	
	@Override
	public void setContactNodes(String... contactNodes) {
		this.contactNode = contactNodes[0];		
	}

	
	@Override
	public void setDefaultPort(int defaultPort) {
		this.defaultPort = String.valueOf(defaultPort);		
	}

	
	@Override
	public void setKeySpace(String keySpace) {	
		//TODO: Not required at the moment
	}
}
