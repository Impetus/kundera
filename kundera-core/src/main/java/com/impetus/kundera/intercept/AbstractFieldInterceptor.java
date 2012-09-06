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

import java.io.Serializable;
import java.util.Set;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */
public abstract class AbstractFieldInterceptor implements FieldInterceptor, Serializable
{
    private Set uninitializedFields;

    private final String entityName;

    private transient boolean initializing;

    private boolean dirty;

    protected AbstractFieldInterceptor(Set uninitializedFields, String entityName)
    {
       
        this.uninitializedFields = uninitializedFields;
        this.entityName = entityName;
    }

      
    public final boolean isInitialized()
    {
        return uninitializedFields == null || uninitializedFields.size() == 0;
    }

    public final boolean isInitialized(String field)
    {
        return uninitializedFields == null || !uninitializedFields.contains(field);
    }

    public final void dirty()
    {
        dirty = true;
    }

    public final boolean isDirty()
    {
        return dirty;
    }

    public final void clearDirty()
    {
        dirty = false;
    }

    protected final Object intercept(Object target, String fieldName, Object value)
    {
        if (initializing)
        {
            return value;
        }

        if (uninitializedFields != null && uninitializedFields.contains(fieldName))
        {       

            final Object result;
            initializing = true;
            try
            {
                /*result = ((LazyPropertyInitializer) session.getFactory().getEntityPersister(entityName))
                        .initializeLazyProperty(fieldName, target, session);*/
                result = null;
            }
            finally
            {
                initializing = false;
            }
            uninitializedFields = null;                                       
            return result;
        }
        else
        {
            return value;
        }
    }

    public final Set getUninitializedFields()
    {
        return uninitializedFields;
    }

    public final String getEntityName()
    {
        return entityName;
    }

    public final boolean isInitializing()
    {
        return initializing;
    }
}
