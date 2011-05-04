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
package com.impetus.kundera.utils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import javax.persistence.PersistenceException;

/**
 * The Class ReflectUtils.
 * 
 * @author animesh.kumar
 */
public class ReflectUtils {

    /**
     * Instantiates a new reflect utils.
     */
    private ReflectUtils() {

    }

    /**
     * Checks for interface "has" in class "in".
     * 
     * @param has
     *            the has
     * @param in
     *            the in
     * 
     * @return true, if exists?
     */
    public static boolean hasInterface(Class<?> has, Class<?> in) {
        if (has.equals(in)) {
            return true;
        }
        boolean match = false;
        for (Class<?> intrface : in.getInterfaces()) {
            if (intrface.getInterfaces().length > 0) {
                match = hasInterface(has, intrface);
            } else {
                match = intrface.equals(has);
            }

            if (match) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets the type arguments.
     * 
     * @param property
     *            the property
     * 
     * @return the type arguments
     */
    public static Type[] getTypeArguments(Field property) {
        Type type = property.getGenericType();
        if (type instanceof ParameterizedType) {
            return ((ParameterizedType) type).getActualTypeArguments();
        }
        return null;
    }

    /**
     * Checks for super "has" in class "in".
     * 
     * @param has
     *            the has
     * @param in
     *            the in
     * 
     * @return true, if exists?
     */
    public static boolean hasSuperClass(Class<?> has, Class<?> in) {
        if (in.equals(has)) {
            return true;
        }
        boolean match = false;
        // stop if the superclass is Object
        if (in.getSuperclass().equals(Object.class)) {
            return match;
        }
        match = hasSuperClass(has, in.getSuperclass());
        return match;
    }

    /**
	 * Loads class with className using classLoader.
	 * 
	 * @param className
	 *            the class name
	 * @param classLoader
	 *            the class loader
	 * @return the class
	 */
    public static Class<?> classForName (String className, ClassLoader classLoader) {
        try {
            Class<?> c = null;
            try {
                c = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                try {
                    c = Class.forName(className);
                } catch (ClassNotFoundException e1) {
                    if(classLoader == null){
                        throw e1;
                    } else {
                        c = classLoader.loadClass(className);
                    }
                }
            }
            return c;
        } catch (ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }

    
	/**
	 * Strip enhancer class.
	 * 
	 * @param c
	 *            the c
	 * @return the class
	 */
	public static Class<?> stripEnhancerClass(Class<?> c) {
		String className = c.getName();

		// strip CGLIB from name
		int enhancedIndex = className.indexOf("$$EnhancerByCGLIB");
		if (enhancedIndex != -1) {
			className = className.substring(0, enhancedIndex);
		}

		if (className.equals(c.getName())) {
			return c;
		} else {
			c = classForName(className, c.getClassLoader());
		}
		return c;
	}

}
