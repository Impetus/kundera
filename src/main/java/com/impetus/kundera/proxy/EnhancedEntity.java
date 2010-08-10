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
package com.impetus.kundera.proxy;

import java.util.Map;
import java.util.Set;

/**
 * Interface to proxy @Entity classes so as to introduce 
 * Foreign relations.
 * 
 * @author animesh.kumar
 *
 */
public interface EnhancedEntity {

	/**
	 * Map to hold foreign relationships
	 * Key=>property
	 * Value=>Set of foreign @Entity ids
	 * 
	 * @return
	 */
	Map<String, Set<String>> getForeignKeysMap();
	
	/**
	 * @Entity that is proxied
	 * 
	 * @return
	 */
	Object getEntity ();

	/**
	 * Id of @Entity object
	 * 
	 * @return
	 */
	String getId();
}
