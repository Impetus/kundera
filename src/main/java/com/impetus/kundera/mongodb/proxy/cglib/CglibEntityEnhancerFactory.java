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
package com.impetus.kundera.proxy.cglib;

import java.util.Map;
import java.util.Set;

import com.impetus.kundera.proxy.EnhancedEntity;
import com.impetus.kundera.proxy.EntityEnhancerFactory;

import net.sf.cglib.proxy.Enhancer;

/**
 * Implementation of EntityEnhancerFactory using cglib library.
 * 
 * @author animesh.kumar
 */
public class CglibEntityEnhancerFactory implements EntityEnhancerFactory {

	/*
	 * @see
	 * com.impetus.kundera.proxy.EntityEnhancerFactory#getProxy(java.lang.Object
	 * , java.lang.String, java.util.Map)
	 */
	@Override
	public EnhancedEntity getProxy(Object entity, String id,
			Map<String, Set<String>> foreignKeyMap) {

		Enhancer e = new Enhancer();
		e.setSuperclass(entity.getClass());
		e.setInterfaces(new Class[] { EnhancedEntity.class });
		e.setCallback(new CglibEnhancedEntity(entity, id, foreignKeyMap));
		return (EnhancedEntity) e.create();
	}

}
