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
 * Abstract class containing common methods for all interfaces extending
 * {@link Collection} interface
 * 
 * @author amresh.singh
 */
public abstract class AbstractProxyCollection extends AbstractProxyBase {

	/**
	 * Default constructor
	 */
	public AbstractProxyCollection() {
		super();
	}

	/**
	 * @param delegator
	 */
	public AbstractProxyCollection(final PersistenceDelegator delegator,
			final Relation relation) {
		super(delegator, relation);
	}

	protected boolean add(final Object object) {
		eagerlyLoadDataCollection();

		boolean result = false;
		if (dataCollection != null && !dataCollection.contains(object)
				&& object != null) {
			getPersistenceDelegator().persist(object);
			dataCollection.add(object);
			result = true;
		}
		return result;
	}

	protected boolean addAll(final Collection collection) {
		eagerlyLoadDataCollection();

		boolean result = false;

		if (dataCollection != null && collection != null
				&& !collection.isEmpty()) {
			for (Object o : collection) {
				if (!dataCollection.contains(o) && o != null) {
					getPersistenceDelegator().persist(o);
					dataCollection.add(o);
				}
			}
			result = true;
		}
		return result;
	}

	protected boolean remove(final Object object) {
		eagerlyLoadDataCollection();

		boolean result = false;
		if (dataCollection != null && object != null) {
			getPersistenceDelegator().remove(object);
			if (dataCollection.contains(object)) {
				dataCollection.remove(object);
			}
			result = true;
		}
		return result;
	}

	protected boolean removeAll(final Collection collection) {
		eagerlyLoadDataCollection();
		boolean result = false;
		if (dataCollection != null && collection != null
				&& !collection.isEmpty()) {
			for (Object o : collection) {
				if (o != null) {
					getPersistenceDelegator().remove(o);
				}
			}

			dataCollection.removeAll(collection);
			result = true;
		}
		return result;
	}

	protected boolean retainAll(final Collection collection) {
		boolean result = false;
		eagerlyLoadDataCollection();

		if (dataCollection != null && collection != null
				&& !collection.isEmpty()) {
			result = dataCollection.retainAll(collection);
		}

		return result;
	}

	protected Iterator iterator() {

    eagerlyLoadDataCollection();
		Iterator result = null;
		if (getDataCollection() != null) {
			result = getDataCollection().iterator();
		}
		return result;
	}

	protected Object[] toArray() {
		eagerlyLoadDataCollection();
		return dataCollection == null ? new Object[0] : dataCollection
				.toArray();
	}

	protected Object[] toArray(final Object[] arg0) {
		eagerlyLoadDataCollection();
		return dataCollection == null ? new Object[0] : dataCollection
				.toArray(arg0);
	}	
}
