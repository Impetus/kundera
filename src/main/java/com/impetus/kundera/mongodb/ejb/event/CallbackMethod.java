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
package com.impetus.kundera.ejb.event;

import java.lang.reflect.InvocationTargetException;

/**
 * Interface that defines how JPA Entity Listeners can be called.
 * 
 * @author animesh.kumar
 */
public interface CallbackMethod {

    /**
     * Invokes the method with entity object.
     * 
     * @param entity
     *            the entity
     * 
     * @throws IllegalArgumentException
     *             the illegal argument exception
     * @throws IllegalAccessException
     *             the illegal access exception
     * @throws InvocationTargetException
     *             the invocation target exception
     * @throws InstantiationException
     *             the instantiation exception
     */
    public void invoke(Object entity) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException;
}
