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
import com.impetus.kundera.mongodb.MongoDBDataHandler;
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
	String dbName;
	private boolean isConnected;
	 
	private EntityManager em;
	
	Mongo mongo;
	DB mongoDb;

	private static Log log = LogFactory.getLog(MongoDBClient.class);


	@Override
	@Deprecated
	public void writeColumns(String dbName, String documentName, String key,
			List<Column> columns, EnhancedEntity e) throws Exception {
		throw new PersistenceException("Not yet implemented");
	}	
	
	@Override
	public void writeColumns(EntityManagerImpl em, EnhancedEntity e, EntityMetadata m) throws Exception {
		String dbName = m.getKeyspaceName();
		String documentName = m.getColumnFamilyName();
		String key = e.getId();
		log.debug("Persisting data into " + dbName + "." + documentName + " for " + key);
		DBCollection dbCollection = mongoDb.getCollection(documentName);	
		
		BasicDBObject document = new MongoDBDataHandler().getDocumentFromEntity(em, m, e.getEntity());	
		dbCollection.insert(document);	
	}	

	
	@Override
	public <E> E loadColumns(EntityManagerImpl em, Class<E> clazz,
			String dbName, String documentName, String key, EntityMetadata m)
			throws Exception {		
		log.debug("Fetching data from " + documentName + " for PK " + key);
		DBCollection dbCollection = mongoDb.getCollection(documentName);
		
		BasicDBObject query = new BasicDBObject();
        query.put(m.getIdColumn().getName(), key);

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;
        
        if(cursor.hasNext()) {
            fetchedDocument = cursor.next();
        } else {
        	return null;
        }
        
        Object entity = new MongoDBDataHandler().getEntityFromDocument(em, clazz, m, fetchedDocument);      
        
        return (E)entity;
	}
	
	

	
	@Override
	public <E> List<E> loadColumns(EntityManagerImpl em, Class<E> clazz,
			String dbName, String documentName, EntityMetadata m,
			String... keys) throws Exception {
		log.debug("Fetching data from " + documentName + " for Keys " + keys);
		
		DBCollection dbCollection = mongoDb.getCollection(documentName);
		
		BasicDBObject query = new BasicDBObject();
        query.put(m.getIdColumn().getName(), new BasicDBObject("$in", keys));			

        DBCursor cursor = dbCollection.find(query);		
        
        List entities = new ArrayList<E>();        
        while(cursor.hasNext()) {
			DBObject fetchedDocument = cursor.next();
			Object entity = new MongoDBDataHandler().getEntityFromDocument(em, clazz, m, fetchedDocument); 
			entities.add(entity);			
		}       
		return entities;
	}
	
	@Override
	public void delete(String idColumnName, String documentName, String rowId)
			throws Exception {		
		DBCollection dbCollection = mongoDb.getCollection(documentName);		
		
		//Find the DBObject to remove first
		BasicDBObject query = new BasicDBObject();
        query.put(idColumnName, rowId);

        DBCursor cursor = dbCollection.find(query);
        DBObject documentToRemove = null;
        
        if(cursor.hasNext()) {
            documentToRemove = cursor.next();
        } else {
        	throw new PersistenceException("Can't remove Row# " + rowId + " for "
        			+ documentName + " because record doesn't exist.");
        }
		
		dbCollection.remove(documentToRemove);				
	}
	
	@Override
	public void connect() {
		if(!isConnected) {
			log.info(">>> Connecting to MONGODB at " + contactNode + " on port " + defaultPort);
			try {
				mongo = new Mongo(contactNode, Integer.parseInt(defaultPort));
				mongoDb = mongo.getDB(dbName);
				isConnected = true;
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

	//For MongoDB, keyspace means DB name
	@Override
	public void setKeySpace(String keySpace) {	
		this.dbName = keySpace;
	}
}
