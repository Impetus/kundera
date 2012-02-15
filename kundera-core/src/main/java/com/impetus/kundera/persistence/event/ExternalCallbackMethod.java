/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.persistence.event;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Class to hold class-method instances for EntityListeners.
 * 
 * @author animesh.kumar
 */
public final class ExternalCallbackMethod implements CallbackMethod
{

    /** The clazz. */
    private Class<?> clazz;

    /** The method. */
    private Method method;

    /**
     * Instantiates a new external callback method.
     * 
     * @param clazz
     *            the clazz
     * @param method
     *            the method
     */
    public ExternalCallbackMethod(Class<?> clazz, Method method)
    {
        this.clazz = clazz;
        this.method = method;
    }

    /*
     * @see
     * com.impetus.kundera.ejb.event.CallbackMethod#invoke(java.lang.Object)
     */
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.ejb.event.CallbackMethod#invoke(java.lang.Object)
     */
    public void invoke(Object entity) throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException, InstantiationException
    {
        if (!method.isAccessible())
            method.setAccessible(true);
        method.invoke(clazz.newInstance(), new Object[] { entity });
    }

    /* @see java.lang.Object#toString() */
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(clazz.getName() + "." + method.getName());
        return builder.toString();
    }
}