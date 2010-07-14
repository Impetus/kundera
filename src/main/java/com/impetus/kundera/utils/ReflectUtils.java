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

/**
 * The Class ReflectUtils.
 * 
 * @author animesh.kumar
 */
public class ReflectUtils {

    /**
     * Checks for interface "has" in class "in"
     * 
     * @param has  
     * @param in
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
     * @return the type arguments
     */
    public static Type[] getTypeArguments(Field property) {
        Type type = property.getGenericType();
        if (type instanceof ParameterizedType) {
            return ((ParameterizedType) type).getActualTypeArguments();
        }
        return null;
    }

}
