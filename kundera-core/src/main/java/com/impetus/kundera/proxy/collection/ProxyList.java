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
import java.util.List;
import java.util.ListIterator;

import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * Proxy class used to represent instances for {@link List}
 * @author amresh.singh
 */
public class ProxyList extends AbstractProxyCollection implements
		ProxyCollection, List {
	
	public ProxyList() {

	}	

	public ProxyList(PersistenceDelegator delegator, Relation relation) {
		super(delegator, relation);
	}
	
	/////////////Methods from Collection interface////////////////////////////
	@Override
	public boolean contains(Object arg0) {
		return super.contains(arg0);
	}

	@Override
	public boolean containsAll(Collection arg0) {
		return super.containsAll(arg0);
	}
	
	@Override
	public void clear() {
		super.clear();
		
	}
	
	@Override
	public boolean isEmpty() {
		return super.isEmpty();
	}
	

	@Override
	public boolean add(Object arg0) {
		return super.add(arg0);
	}
	
	@Override
	public boolean addAll(Collection arg0) {
		return super.addAll(arg0);
	}
	
	@Override
	public boolean remove(Object arg0) {
		return super.remove(arg0);
	}
	
	@Override
	public boolean removeAll(Collection c) {
		return super.removeAll(c);
	}
	
	@Override
	public boolean retainAll(Collection c) {
		return super.retainAll(c);
	}
	
	@Override
	public Iterator iterator() {
		return super.iterator();
	}
	
	@Override
	public Object[] toArray() {
		return super.toArray();
	}

	@Override
	public Object[] toArray(Object[] arg0) {
		return super.toArray(arg0);
	}
	
	@Override
	public int size() {
		return super.size();
	}
	
	//////////////Methods from List interface ///////////////////////

	@Override
	public void add(int arg0, Object arg1) {
		eagerlyLoadDataCollection();
		
		List dataList = (List) dataCollection;
		
		if(dataList != null && ! dataList.contains(arg1) && arg1  != null)
		{
			getPersistenceDelegator().persist(arg1);
			dataList.add(arg0, arg1);			
		}	
	}	

	@Override
	public boolean addAll(int i, Collection c) {
		eagerlyLoadDataCollection();
		
		List dataList = (List) dataCollection;
		
		if(dataList != null && c != null && ! c.isEmpty())
		{
			int j = 0;
			for(Object o : c)
			{
				if(! dataList.contains(o) && o != null)
				{
					getPersistenceDelegator().persist(o);
					dataList.add(i + j++, o);					
				}
			}
			return true;
		}
		return false;
	}
	
	@Override
	public Object get(int arg0) {
		eagerlyLoadDataCollection();
		
		List dataList = (List) dataCollection;
		if(dataList == null || dataList.isEmpty())
		{
			return null;
		}
		return dataList.get(arg0);
	}

	@Override
	public int indexOf(Object arg0) {
		eagerlyLoadDataCollection();
		
		List dataList = (List) dataCollection;
		if(dataList == null || dataList.isEmpty())
		{
			return -1;
		}
		return dataList.indexOf(arg0);
	}		

	@Override
	public int lastIndexOf(Object arg0) {
		eagerlyLoadDataCollection();
		
		List dataList = (List) dataCollection;
		if(dataList == null || dataList.isEmpty())
		{
			return -1;
		}
		return dataList.lastIndexOf(arg0);
	}

	@Override
	public ListIterator listIterator() {
		eagerlyLoadDataCollection();
		
		List dataList = (List) dataCollection;
		if(dataList == null || dataList.isEmpty())
		{
			return null;
		}
		return dataList.listIterator();
	}

	@Override
	public ListIterator listIterator(int arg0) {
		eagerlyLoadDataCollection();
		
		List dataList = (List) dataCollection;
		if(dataList == null || dataList.isEmpty())
		{
			return null;
		}
		return dataList.listIterator(arg0);
	}

	@Override
	public Object remove(int arg0) {
		eagerlyLoadDataCollection();
		
		List dataList = (List) dataCollection;
		if(dataList == null || dataList.isEmpty() && ! dataList.contains(arg0))
		{
			return null;
		}
		
		Object entity = dataList.get(arg0);				
		getPersistenceDelegator().remove(arg0);
		
		return dataList.remove(arg0);	
			
	}	

	@Override
	public Object set(int arg0, Object arg1) {
		eagerlyLoadDataCollection();		
		List dataList = (List) dataCollection;
		
		if(dataList == null || dataList.isEmpty())
		{
			return null;
		}
		
		if(dataList.get(arg0) == null)
		{
			getPersistenceDelegator().persist(arg1);
			return dataList.set(arg0, arg1);
			
		}
		else
		{
			getPersistenceDelegator().merge(arg1);
			return dataList.set(arg0, arg1);
		}			
	}	

	@Override
	public List subList(int arg0, int arg1) {
		eagerlyLoadDataCollection();
		
		List dataList = (List) dataCollection;
		if(dataList == null || dataList.isEmpty())
		{
			return null;
		}
		return dataList.subList(arg0, arg1);
	}	

}
