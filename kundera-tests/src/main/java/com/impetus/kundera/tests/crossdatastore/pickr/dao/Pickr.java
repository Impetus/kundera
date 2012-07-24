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
package com.impetus.kundera.tests.crossdatastore.pickr.dao;

import java.util.List;

/**
 * Interface for Pickr application
 * 
 * @author amresh.singh
 */
public interface Pickr
{
    void addPhotographer(Object p);

    Object getPhotographer(Class<?> entityClass, String photographerId);

    public List<Object> getAllPhotographers(String className);

    public void deletePhotographer(Object p);

    public void mergePhotographer(Object p);

    public void close();
}
