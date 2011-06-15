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
package com.impetus.kundera.index;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Client;
import com.impetus.kundera.client.MongoDBClient;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.PropertyIndex;

/**
 * Provides indexing functionality for MongoDB database 
 * @author amresh.singh
 */
public class MongoDBIndexer implements Indexer {
	/** log for this class. */
	private static Log log = LogFactory.getLog(MongoDBIndexer.class);
	MongoDBClient client;
	
	public MongoDBIndexer(Client client) {
		this.client = (MongoDBClient) client;
	}

	@Override
	public void unindex(EntityMetadata metadata, String id) {
		log.debug("No need to remove data from Index. It's handled automatically by MongoDB when document is dropped");		
	}


	@Override
	public void index(EntityMetadata metadata, Object object) {
		if (!metadata.isIndexable()) {
			return;
		}

		log.debug("Indexing @Entity[" + metadata.getEntityClazz().getName() + "] " + object);
		String indexName = metadata.getIndexName();	//Index Name=Collection name, not required
		
		List<PropertyIndex> indexProperties = metadata.getIndexProperties();
		
		List<String> columnList = new ArrayList<String>();
		
		for(PropertyIndex propertyIndex : indexProperties) {			
			columnList.add(propertyIndex.getName());
		}	
		
		client.createIndex(metadata.getTableName(), columnList, 1);	//1=Ascending
				
	}


	@Override
	public List<String> search(String query, int start, int count) {
		throw new PersistenceException("Invalid method call! When you search on a column, MongoDB will automatically search in index if that exists.");		
	}
	
}
