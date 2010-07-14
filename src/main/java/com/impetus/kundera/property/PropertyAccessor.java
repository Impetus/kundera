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
package com.impetus.kundera.property;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Interface to access {@link Field} property of a java class.
 * 
 * @author animesh.kumar
 */
public interface PropertyAccessor<T> {

    /**
     * Gets a byte array value of a Field.
     * 
     * @param from
     *            Object to access the field from
     * @param property
     *            Field
     * @param alias
     *            Cassandra-column-name of this Field. By default, this is same
     *            as Field's name
     * 
     * @return Map (alias->byte[] value)
     * 
     * @throws IllegalArgumentException
     *             * @throws IllegalAccessException * @throws
     *             PropertyAccessException the property access exception
     */
    Map<String, byte[]> readAsByteArray (Object from, Field property, String alias) throws PropertyAccessException;

	/**
	 * @param target
	 * @param property
	 * @param alias
	 * @return
	 * @throws PropertyAccessException
	 */
	Map<String, T> readAsObject(Object target, Field property, String alias)
			throws PropertyAccessException;
	
	/**
     * Sets the.
     * 
     * @param target
     *            Target object
     * @param property
     *            Field
     * @param bytes
     *            byte-array value to be set
     * @param alias
     *            Cassandra-column-name of this Field. By default, this is same
     *            as Field's name
     * 
     * @throws IllegalArgumentException
     *             * @throws IllegalAccessException * @throws
     *             PropertyAccessException the property access exception
     */
    void set(Object target, Field property, byte[] bytes, String alias) throws PropertyAccessException;

    /**
     * Decode.
     * 
     * @param b
     *            the b
     * 
     * @return the T
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    T fromBytes(byte[] b) throws PropertyAccessException;

    /**
     * Encode.
     * 
     * @param value
     *            the value
     * 
     * @return the byte[]
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    byte[] toBytes(T value) throws PropertyAccessException;



}
