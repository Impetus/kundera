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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.sf.cglib.proxy.InvocationHandler;

/**
 * Implementation of EnhancedEntity using cglib library.
 * 
 * @author animesh.kumar
 */
public class CglibEnhancedEntity implements InvocationHandler, EnhancedEntity {

	/** The entity. */
	private Object entity;

	/** The id. */
	private String id;

	/** The map. */
	private Map<String, Set<String>> map = new HashMap<String, Set<String>>();

	/**
	 * Instantiates a new cglib enhanced entity.
	 * 
	 * @param entity
	 *            the entity
	 * @param id
	 *            the id
	 * @param foreignKeysMap
	 *            the foreign keys map
	 */
	public CglibEnhancedEntity(final Object entity, final String id,
			final Map<String, Set<String>> foreignKeysMap) {
		this.entity = entity;
		this.id = id;
		this.map = foreignKeysMap;
	}

	/*
	 * @see net.sf.cglib.proxy.InvocationHandler#invoke(java.lang.Object,
	 * java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {

		String methodName = method.getName();
		int params = args.length;

		if (params == 0 && "getForeignKeysMap".equals(methodName)) {
			return getForeignKeysMap();
		} else if (params == 0 && "toString".equals(methodName)) {
			return toString();
		} else if (params == 0 && "getEntity".equals(methodName)) {
			return getEntity();
		} else if (params == 0 && "getId".equals(methodName)) {
			return getId();
		}

		return method.invoke(entity, args);
	}

	/* @see com.impetus.kundera.proxy.EnhancedEntity#getForeignKeysMap() */
	public Map<String, Set<String>> getForeignKeysMap() {
		return map;
	}

	/* @see com.impetus.kundera.proxy.EnhancedEntity#getEntity() */
	@Override
	public Object getEntity() {
		return entity;
	}

	/**
	 * Gets the id.
	 * 
	 * @return the id
	 */
	@Override
	public String getId() {
		return id;
	}

	/* @see java.lang.Object#toString() */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Proxy [id=");
		builder.append(id);
		builder.append(", entity=");
		builder.append(entity);
		builder.append(", foreignKeys=");
		builder.append(map);
		builder.append("]");
		return builder.toString();
	}

}
