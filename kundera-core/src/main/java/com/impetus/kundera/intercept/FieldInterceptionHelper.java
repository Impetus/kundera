/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.intercept;

import java.util.Set;

import net.sf.cglib.transform.impl.InterceptFieldEnabled;

import com.impetus.kundera.proxy.cglib.CglibFieldInterceptorImpl;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */
public class FieldInterceptionHelper
{
    private FieldInterceptionHelper() {
    }

    public static boolean isInstrumented(Class entityClass) {
        Class[] definedInterfaces = entityClass.getInterfaces();
        for ( int i = 0; i < definedInterfaces.length; i++ ) {
            if (InterceptFieldEnabled.class.getSimpleName().equals(definedInterfaces[i].getName())) {
                return true;
            }
        }
        return false;
    }

    public static boolean isInstrumented(Object entity) {
        return entity != null && isInstrumented(entity.getClass() );
    }

    public static FieldInterceptor extractFieldInterceptor(Object entity) {
        if ( entity == null ) {
            return null;
        }
        Class[] definedInterfaces = entity.getClass().getInterfaces();
        for ( int i = 0; i < definedInterfaces.length; i++ ) {
            if (InterceptFieldEnabled.class.getSimpleName().equals( definedInterfaces[i].getName() ) ) {
                // we have a CGLIB enhanced entity
                return ((FieldInterceptor)((InterceptFieldEnabled)entity).getInterceptFieldCallback());
            }            
        }
        return null;
    }

    public static FieldInterceptor injectFieldInterceptor(
            Object entity,
            String entityName,
            Set uninitializedFieldNames
            ) {
        if ( entity != null ) {
            Class[] definedInterfaces = entity.getClass().getInterfaces();
            for ( int i = 0; i < definedInterfaces.length; i++ ) {
                if (InterceptFieldEnabled.class.getSimpleName().equals(definedInterfaces[i].getName() ) ) {
                    // we have a CGLIB enhanced entity
                    CglibFieldInterceptorImpl fieldInterceptor = new CglibFieldInterceptorImpl(uninitializedFieldNames,
                            entityName);
                    ((InterceptFieldEnabled) entity).setInterceptFieldCallback(fieldInterceptor);
                    return fieldInterceptor;                   
                }                
            }
        }
        return null;
    }

    public static void clearDirty(Object entity) {
        FieldInterceptor interceptor = extractFieldInterceptor( entity );
        if ( interceptor != null ) {
            interceptor.clearDirty();
        }
    }

    public static void markDirty(Object entity) {
        FieldInterceptor interceptor = extractFieldInterceptor( entity );
        if ( interceptor != null ) {
            interceptor.dirty();
        }
    }

}
