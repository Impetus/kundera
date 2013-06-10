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
import java.util.Set;

import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * Proxy class used to represent instances for {@link Map}
 * @author amresh.singh
 */
public class ProxyMap extends ProxyBase implements
		ProxyCollection, Map {
	
	public ProxyMap() {

	}	

	public ProxyMap(PersistenceDelegator delegator, Relation relation) {
		super(delegator, relation);
	}	
	
	/////////////////////////Methods from Collection interface	////////////////

	@Override
	public void clear() {
		super.clear();
	}
	
	@Override
	public boolean isEmpty() {
		return super.isEmpty();
	}
	
	@Override
	public int size() {
		return super.size();
	}

	/////////////////////////Methods from Map interface	////////////////
	
	@Override
	public boolean containsKey(Object arg0) {		
		eagerlyLoadDataCollection();		
		Map dataMap = (Map) dataCollection;
		
		if(dataMap == null || dataMap.isEmpty())
		{
			return false;
		}
		return dataMap.containsKey(arg0);
	}

	@Override
	public boolean containsValue(Object arg0) {
		eagerlyLoadDataCollection();		
		Map dataMap = (Map) dataCollection;
		
		if(dataMap == null || dataMap.isEmpty())
		{
			return false;
		}
		return dataMap.containsValue(arg0);
	}

	@Override
	public Set entrySet() {
		eagerlyLoadDataCollection();		
		Map dataMap = (Map) dataCollection;
		
		if(dataMap == null || dataMap.isEmpty())
		{
			return null;
		}
		return dataMap.entrySet();
	}

	@Override
	public Object get(Object arg0) {
		eagerlyLoadDataCollection();		
		Map dataMap = (Map) dataCollection;
		
		if(dataMap == null || dataMap.isEmpty())
		{
			return null;
		}
		return dataMap.get(arg0);
	}	

	@Override
	public Set keySet() {
		eagerlyLoadDataCollection();		
		Map dataMap = (Map) dataCollection;
		
		if(dataMap == null || dataMap.isEmpty())
		{
			return null;
		}
		return dataMap.keySet();
	}

	@Override
	public Object put(Object arg0, Object arg1) {
		eagerlyLoadDataCollection();		
		Map dataMap = (Map) dataCollection;
		
		if(dataMap == null || dataMap.isEmpty())
		{
			return null;
		}	
		return dataMap.put(arg0, arg1);
	}

	@Override
	public void putAll(Map arg0) {
		eagerlyLoadDataCollection();		
		Map dataMap = (Map) dataCollection;
		
		if(dataMap != null && dataMap.isEmpty())
		{
			dataMap.putAll(arg0);
		}	

	}

	@Override
	public Object remove(Object arg0) {
		eagerlyLoadDataCollection();		
		Map dataMap = (Map) dataCollection;
		
		if(dataMap == null || dataMap.isEmpty())
		{
			return null;
		}
		return dataMap.remove(arg0);
	}	

	@Override
	public Collection values() {
		eagerlyLoadDataCollection();		
		Map dataMap = (Map) dataCollection;
		
		if(dataMap == null || dataMap.isEmpty())
		{
			return null;
		}
		return dataMap.values();
	}

}
