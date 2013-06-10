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
import java.util.HashMap;
import java.util.Map;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.AssociationBuilder;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Abstract class containing common methods for all interfaces extending {@link Collection} interface and {@link Map} interface
 * @author amresh.singh
 */
public abstract class ProxyBase implements ProxyCollection {
	
	private PersistenceDelegator delegator;
	private Object owner;
	private Map<String, Object> relationsMap;
	private Relation relation;
	
	protected Collection dataCollection;

	public ProxyBase() {
	}
	
	@Override
	public ProxyCollection getCopy() {
		ProxyCollection proxyCollection = new ProxySet(getPersistenceDelegator(), getRelation());
		proxyCollection.setRelationsMap(getRelationsMap());
		return proxyCollection;
	}

	/**
	 * @param delegator
	 */
	public ProxyBase(PersistenceDelegator delegator, Relation relation) {
		this.delegator = delegator;
		this.relation = relation;
	}

	@Override
	public Object getOwner() {
		return owner;
	}

	@Override
	public void setOwner(Object owner) {
		this.owner = owner;
	}

	@Override
	public PersistenceDelegator getPersistenceDelegator() {
		return delegator;
	}

	@Override
	public void setPersistenceDelegator(PersistenceDelegator delegator) {
		this.delegator = delegator;

	}

	@Override
	public void addRelationToMap(String relationName, Object relationValue) {
		if(relationsMap == null)
		{
			relationsMap = new HashMap<String, Object>();			
		}
		relationsMap.put(relationName, relationValue);
		
	}

	@Override
	public Map<String, Object> getRelationsMap() {
		return relationsMap;
	}

	@Override
	public void setRelationsMap(Map<String, Object> relationsMap) {
		this.relationsMap = relationsMap;		
	}

	@Override
	public Object getRelationValue(String relationName) {
		if(relationsMap == null) return null;
		return relationsMap.get(relationName);
	}

	@Override
	public Relation getRelation() {
		return relation;
	}

	@Override
	public void setRelation(Relation relation) {
		this.relation = relation;		
	}	
	
	@Override
	public Collection getDataCollection() {
		return dataCollection;
	}

	@Override
	public void setDataCollection(Collection dataCollection) {
		this.dataCollection = dataCollection;		
	}	

	/**
	 * 
	 */
	protected void eagerlyLoadDataCollection() {
		if(dataCollection == null)
		{
			EntityMetadata m = KunderaMetadataManager.getEntityMetadata(getOwner().getClass());
			Object entityId = PropertyAccessorHelper.getId(getOwner(), m);
			new AssociationBuilder().setConcreteRelationObject(getOwner(), getRelationsMap(), m, getPersistenceDelegator(), entityId, getRelation());
			
			dataCollection = (Collection)PropertyAccessorHelper.getObject(getOwner(), getRelation().getProperty());			
			PropertyAccessorHelper.set(getOwner(), getRelation().getProperty(), dataCollection);
		}	
	}	
	
	
	/////////////////////Common collection implementation////////////////////////////	
	protected void clear()
	{
		eagerlyLoadDataCollection();
		if (getDataCollection() != null) {
			getDataCollection().clear();
		}
	}
	
	protected boolean contains(Object arg0) {
		
		eagerlyLoadDataCollection();
		if(getDataCollection() != null)
		{
			return getDataCollection().contains(arg0);
		}		
		return false;
	}
	
	protected boolean containsAll(Collection arg0) {
		eagerlyLoadDataCollection();
		if(getDataCollection() != null)
		{
			return getDataCollection().containsAll(arg0);
		}
		return false;
	}
	
	protected boolean isEmpty() {
		eagerlyLoadDataCollection();	
		if(getDataCollection() != null)
		{
			return getDataCollection().isEmpty();
		}
		return true;
	}
	
	protected int size() {		
		eagerlyLoadDataCollection();	
		return dataCollection == null ? 0 : dataCollection.size();
	}
}
