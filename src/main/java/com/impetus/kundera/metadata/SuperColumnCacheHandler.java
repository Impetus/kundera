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

import com.impetus.kundera.entity.Tweet;


/**
 * Cache for holding super column name and object 
 * @author amresh.singh
 */
public class SuperColumnCacheHandler {
	/** log for this class. */
	private static Log log = LogFactory.getLog(SuperColumnCacheHandler.class);
	
	/**
	 * Mapping betwwen Row Key and (Map of super column Object and super column name)  
	 */
	private Map<String, Map<Object, String>> superColumnCache;

	/**
	 * @return the superColumnMaps
	 */
	public Map<String, Map<Object, String>> getSuperColumnCache() {
		if(this.superColumnCache == null) {
			this.superColumnCache = new HashMap<String, Map<Object, String>>();
		}
		return this.superColumnCache;
	}	
	
	public void addSuperColumnMapping(String rowKey, Object superColumnObject, String superColumnName) {
		Map superColumnMap = new HashMap<Object, String>();
		if(getSuperColumnCache().get(rowKey) == null) {			
			superColumnMap.put(superColumnObject, superColumnName);			
			getSuperColumnCache().put(rowKey, superColumnMap);
		} else {
			getSuperColumnCache().get(rowKey).put(superColumnObject, superColumnName);
		}
	}
	
	public String getSuperColumnName(String rowKey, Object superColumnObject) {
		if(getSuperColumnCache().get(rowKey) == null) {
			log.debug("No super column map found in cache for Row key " + rowKey);
			return null;
		} else {
			Map<Object, String> superColumnMap = getSuperColumnCache().get(rowKey);
			String superColumnName = superColumnMap.get(superColumnObject);
			if(superColumnName == null) {
				log.debug("No super column name found in cache for object:" + superColumnObject);
				return null;
			} else {
				return superColumnName;
			}			
		}
	}
	
	public void clearCache() {
		this.superColumnCache = null;
	}
	
	public static void main(String args[]) {
		SuperColumnCacheHandler h = new SuperColumnCacheHandler();
		Tweet t1 = new Tweet("1", "Tweet 1111", "web");
		Tweet t2 = new Tweet("2", "Tweet 2222", "mobile");
		Tweet t3 = new Tweet("3", "Tweet 3333", "iPhone");
		
		h.addSuperColumnMapping("IIIPL-0001", t1, "tweet#1");		
		h.addSuperColumnMapping("IIIPL-0001", t2, "tweet#2");		
		h.addSuperColumnMapping("IIIPL-0002", t3, "tweet#a");
		System.out.println(h.getSuperColumnCache());
		
		System.out.println(h.getSuperColumnName("IIIPL-0001", t3));
		
		
	}

}
