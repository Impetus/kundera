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
package com.impetus.kundera.metadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.entity.Tweet;


/**
 * Cache for holding super column name and object 
 * @author amresh.singh
 */
public class EmbeddedCollectionCacheHandler {
	/** log for this class. */
	private static Log log = LogFactory.getLog(EmbeddedCollectionCacheHandler.class);
	
	/**
	 * Mapping betwwen Row Key and (Map of embedded Object of embedded collection and embedded object name)  
	 */
	private Map<String, Map<Object, String>> embeddedCollectionCache;

	/**
	 * @return the superColumnMaps
	 */
	public Map<String, Map<Object, String>> getEmbeddedCollectionCache() {
		if(this.embeddedCollectionCache == null) {
			this.embeddedCollectionCache = new HashMap<String, Map<Object, String>>();
		}
		return this.embeddedCollectionCache;
	}	
	
	public boolean isCacheEmpty() {
		return embeddedCollectionCache == null || embeddedCollectionCache.isEmpty();
	}
	
	public void addEmbeddedCollectionCacheMapping(String rowKey, Object embeddedObject, String embeddedObjectName) {
		Map embeddedObjectMap = new HashMap<Object, String>();
		if(getEmbeddedCollectionCache().get(rowKey) == null) {			
			embeddedObjectMap.put(embeddedObject, embeddedObjectName);			
			getEmbeddedCollectionCache().put(rowKey, embeddedObjectMap);
		} else {
			getEmbeddedCollectionCache().get(rowKey).put(embeddedObject, embeddedObjectName);
		}
	}
	
	public String getEmbeddedObjectName(String rowKey, Object embeddedObject) {
		if(getEmbeddedCollectionCache().get(rowKey) == null) {
			log.debug("No embedded object map found in cache for Row key " + rowKey);
			return null;
		} else {
			Map<Object, String> embeddedObjectMap = getEmbeddedCollectionCache().get(rowKey);
			String embeddedObjectName = embeddedObjectMap.get(embeddedObject);
			if(embeddedObjectName == null) {
				log.debug("No embedded object name found in cache for object:" + embeddedObject);
				return null;
			} else {
				return embeddedObjectName;
			}			
		}
	}
	
	public int getLastEmbeddedObjectCount(String rowKey) {
		if(getEmbeddedCollectionCache().get(rowKey) == null) {
			log.debug("No embedded object map found in cache for Row key " + rowKey);
			return -1;
		} else {
			Map<Object, String> embeddedObjectMap = getEmbeddedCollectionCache().get(rowKey);
			Collection<String> embeddedObjectNames = embeddedObjectMap.values();
			int max = 0;
			
			for (String s : embeddedObjectNames) {
				String embeddedObjectCountStr = s.substring(s.indexOf(Constants.SUPER_COLUMN_NAME_DELIMITER) + 1);
				int embeddedObjectCount = 0;
				try {
					embeddedObjectCount = Integer.parseInt(embeddedObjectCountStr);
				} catch (NumberFormatException e) {
					log.error("Invalid Embedded Object name " + s);
					throw new PersistenceException("Invalid Embedded Object name " + s);
				}
				if(embeddedObjectCount > max) {
					max = embeddedObjectCount;
				}
			}
			return max;
		}
	}
	
	public void clearCache() {
		this.embeddedCollectionCache = null;
	}
	
	public static void main(String args[]) {
		EmbeddedCollectionCacheHandler h = new EmbeddedCollectionCacheHandler();
		Tweet t1 = new Tweet("1", "Tweet 1111", "web");
		Tweet t2 = new Tweet("2", "Tweet 2222", "mobile");
		Tweet t3 = new Tweet("3", "Tweet 3333", "iPhone");
		
		h.addEmbeddedCollectionCacheMapping("IIIPL-0001", t1, "tweet#1");		
		h.addEmbeddedCollectionCacheMapping("IIIPL-0001", t2, "tweet#2");		
		h.addEmbeddedCollectionCacheMapping("IIIPL-0002", t3, "tweet#a");
		System.out.println(h.getEmbeddedCollectionCache());
		
		System.out.println(h.getEmbeddedObjectName("IIIPL-0001", t3));
		System.out.println(h.getLastEmbeddedObjectCount("IIIPL-0001"));
		
		
	}

}
