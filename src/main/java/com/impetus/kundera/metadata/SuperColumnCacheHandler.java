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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Cache for holding super column name and object 
 * @author amresh.singh
 */
public class SuperColumnCacheHandler {
	/** log for this class. */
	private static Log log = LogFactory.getLog(SuperColumnCacheHandler.class);
	
	/**
	 * Mapping betwwen Row Key and (Map of super column name and super column object)  
	 */
	private Map<String, Map<String, Object>> superColumnCache;

	/**
	 * @return the superColumnMaps
	 */
	public Map<String, Map<String, Object>> getSuperColumnCache() {
		if(this.superColumnCache == null) {
			this.superColumnCache = new HashMap<String, Map<String,Object>>();
		}
		return this.superColumnCache;
	}	
	
	public void addSuperColumnMapping(String rowKey, String superColumnName, Object superColumnObject) {
		Map superColumnMap = new HashMap<String, Object>();
		if(getSuperColumnCache().get(rowKey) == null) {			
			superColumnMap.put(superColumnName, superColumnObject);			
			getSuperColumnCache().put(rowKey, superColumnMap);
		} else {
			getSuperColumnCache().get(rowKey).put(superColumnName, superColumnObject);
		}
	}
	
	public Object getSuperColumnObject(String rowKey, String superColumnName) {
		if(getSuperColumnCache().get(rowKey) == null) {
			log.debug("No super column object found in cache for Row key " + rowKey);
			return null;
		} else {
			Map superColumnMap = getSuperColumnCache().get(rowKey);
			Object superColumnObject = superColumnMap.get(superColumnName);
			if(superColumnObject == null) {
				log.debug("No super column object found in cache for name:" + superColumnName );
				return null;
			} else {
				return superColumnObject;
			}			
		}
	}
	
	public static void main(String args[]) {
		SuperColumnCacheHandler h = new SuperColumnCacheHandler();
		h.addSuperColumnMapping("IIIPL-0001", "tweet#1", "Heyyyyyyyyy");
		System.out.println(h.getSuperColumnCache());
		h.addSuperColumnMapping("IIIPL-0001", "tweet#2", "Wowwwwwwwwwww");
		System.out.println(h.getSuperColumnCache());
		h.addSuperColumnMapping("IIIPL-0002", "tweet#a", "Wowwwwwwwwwww");
		System.out.println(h.getSuperColumnCache());
	}

}
