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
import java.util.Set;

import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * Proxy class used to represent instances for {@link Set}
 * @author amresh.singh
 */
public class ProxySet extends AbstractProxyCollection implements
		ProxyCollection, Set {

	public ProxySet() {

	}	

	public ProxySet(PersistenceDelegator delegator, Relation relation) {
		super(delegator, relation);
	}
	
	
	/////////////Methods from Collection interface////////////////////////////
	@Override
	public void clear() {
		super.clear();
	}

	@Override
	public boolean contains(Object arg0) {
		return super.contains(arg0);
	}

	@Override
	public boolean containsAll(Collection arg0) {
		return super.containsAll(arg0);
	}

	@Override
	public boolean isEmpty() {
		return super.isEmpty();
	}
	
	@Override
	public int size() {		
		return super.size();
	}
	
	@Override
	public boolean add(Object object) {
		return super.add(object);
	}

	@Override
	public boolean addAll(Collection c) {
		return super.addAll(c);
	}
	
	@Override
	public boolean remove(Object object) {
		return super.remove(object);
	}

	@Override
	public boolean removeAll(Collection c) {
		return super.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection  c) {
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
}
