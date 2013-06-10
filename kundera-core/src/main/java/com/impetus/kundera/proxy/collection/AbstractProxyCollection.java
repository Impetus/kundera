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
import java.util.Iterator;

import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * Abstract class containing common methods for all interfaces extending {@link Collection} interface
 * @author amresh.singh
 */
public class AbstractProxyCollection extends ProxyBase {
	
	public AbstractProxyCollection() {
		
	}	
	
	/**
	 * @param delegator
	 */
	public AbstractProxyCollection(PersistenceDelegator delegator, Relation relation) {
		super(delegator, relation);
	}
	
	protected boolean add(Object object) {
		eagerlyLoadDataCollection();
		if(dataCollection != null && ! dataCollection.contains(object) && object  != null)
		{
			getPersistenceDelegator().persist(object);
			dataCollection.add(object);
			return true;
		}		
		return false;
	}
	
	protected boolean addAll(Collection c) {
		eagerlyLoadDataCollection();
		if(dataCollection != null && c != null && ! c.isEmpty())
		{
			for(Object o : c)
			{
				if(! dataCollection.contains(o) && o != null)
				{
					getPersistenceDelegator().persist(o);
					dataCollection.add(o);					
				}
			}
			return true;
		}
		return false;
	}
	
	protected boolean remove(Object object) {
		eagerlyLoadDataCollection();
		if(dataCollection != null && object != null)
		{
			getPersistenceDelegator().remove(object);
			if(dataCollection.contains(object))
			{
				dataCollection.remove(object);
			}
			return true;
		}
		return false;
	}
	
	protected boolean removeAll(Collection c) {
		eagerlyLoadDataCollection();
		if(dataCollection != null && c != null && ! c.isEmpty())
		{
			for(Object o : c)
			{
				if(o != null) {
					getPersistenceDelegator().remove(o);
				}
			}
			
			dataCollection.removeAll(c);
			return true;
		}
		return false;
	}
	
	protected boolean retainAll(Collection  c) {
		eagerlyLoadDataCollection();
		if (dataCollection != null && c != null && !c.isEmpty()) {
			return dataCollection.retainAll(c);
		}
		return false;
	}
	
	protected Iterator iterator() {
		if(getDataCollection() != null)
		{
			eagerlyLoadDataCollection();	
			return getDataCollection().iterator();
		}
		return null;
	}
	
	protected Object[] toArray() {		
		eagerlyLoadDataCollection();
		return dataCollection == null ? new Object[0] : dataCollection.toArray();
	}

	protected Object[] toArray(Object[] arg0) {		
		eagerlyLoadDataCollection();
		return dataCollection == null ? new Object[0] : dataCollection.toArray(arg0);
	}
}
