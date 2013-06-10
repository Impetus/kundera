/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.proxy.collection;

import java.util.Collection;
import java.util.Map;

import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * Top level interface for all proxy collections (including maps)
 * @author amresh.singh
 */
public interface ProxyCollection {

	Object getOwner();

	void setOwner(Object owner);

	PersistenceDelegator getPersistenceDelegator();

	void setPersistenceDelegator(PersistenceDelegator delegator);

	void addRelationToMap(String relationName, Object relationValue);
	Map<String, Object> getRelationsMap();
	void setRelationsMap(Map<String, Object> relationsMap);
	Object getRelationValue(String relationName);

	Relation getRelation();
	void setRelation(Relation relation);
	
	Collection getDataCollection();
	void setDataCollection(Collection dataCollection);
	
	ProxyCollection getCopy();
}
