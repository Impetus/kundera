/*
 * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.mongodb;

import java.lang.reflect.Field;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.mongodb.BasicDBObject;

/**
 * Provides functionality for mapping between MongoDB documents and POJOs.
 * Contains utility methods for converting one form into another. 
 * @author amresh.singh
 */
public class DocumentObjectMapper {
	private static Log log = LogFactory.getLog(DocumentObjectMapper.class);
	
	/**
	 * Creates a MongoDB document list from a given java collection. 
	 * columns in the documents correspond to field names of java objects in collection.  
	 * @throws PropertyAccessException
	 */
	public static BasicDBObject[] getDocumentListFromCollection(Collection coll) throws PropertyAccessException {
		BasicDBObject[] dBObjects = new BasicDBObject[coll.size()];
		int count = 0;
		for(Object o : coll) {		
			dBObjects[count] = getDocumentFromObject(o);
			count++;
		}
		return dBObjects;
	}
	
	/**
	 * Creates a MongoDB document object wrt a given Java object. columns in the document correspond to field names in java object. 
	 * @throws PropertyAccessException
	 */
	public static BasicDBObject getDocumentFromObject(Object obj) throws PropertyAccessException {
		BasicDBObject dBObj = new BasicDBObject();
		Field[] fieldsInObject = obj.getClass().getDeclaredFields();
		for(int i = 0; i < fieldsInObject.length; i++) {
			Field f = fieldsInObject[i];
			Object val = PropertyAccessorHelper.getObject(obj, f);
			dBObj.put(f.getName(), val);
		}
		return dBObj;		
	}

}
