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

import javax.persistence.PersistenceException;

import com.impetus.kundera.ejb.EntityManagerImpl;

/**
 * Implementation of LazyInitializerFactory using cglib library.
 * 
 * @author animesh.kumar
 */
public class CglibLazyInitializerFactory implements LazyInitializerFactory {

	/*
	 * @see
	 * com.impetus.kundera.proxy.LazyInitializerFactory#getProxy(java.lang.String
	 * , java.lang.Class, java.lang.reflect.Method, java.lang.reflect.Method,
	 * java.lang.String, com.impetus.kundera.ejb.EntityManagerImpl)
	 */
	@Override
	public KunderaProxy getProxy(String entityName, Class<?> persistentClass,
			Method getIdentifierMethod, Method setIdentifierMethod, String id,
			EntityManagerImpl em) throws PersistenceException {

		return (KunderaProxy) CglibLazyInitializer.getProxy(entityName,
				persistentClass, new Class[] { KunderaProxy.class },
				getIdentifierMethod, setIdentifierMethod, id, em);

	}

}
