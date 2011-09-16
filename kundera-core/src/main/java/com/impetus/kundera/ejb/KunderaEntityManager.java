/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.ejb;

import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;

import com.impetus.kundera.Client;

/**
 * The Interface KunderaEntityManager.
 * 
 * @author animesh.kumar
 */
public interface KunderaEntityManager extends EntityManager {

	/**
	 * Find.
	 * 
	 * @param <T>
	 *            the generic type
	 * @param entityClass
	 *            the entity class
	 * @param primaryKey
	 *            the primary key
	 * @return the list
	 */
	<T> List<T> find(Class<T> entityClass, Object... primaryKey);

	/**
	 * Find.
	 * 
	 * @param <T>
	 *            the generic type
	 * @param entityClass
	 *            the entity class
	 * @param primaryKeys
	 *            the primary keys
	 * @return the list
	 */
	<T> List<T> find(Class<T> entityClass, Map<String, String> primaryKeys);

	/**
	 * Gets the client.
	 * 
	 * @return the client
	 */
	Client getClient();

}
