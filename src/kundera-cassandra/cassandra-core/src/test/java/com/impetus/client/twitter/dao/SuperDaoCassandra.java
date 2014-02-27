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
package com.impetus.client.twitter.dao;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * The Class SuperDao.
 * 
 * @author impetus
 */
public class SuperDaoCassandra
{

    /**
     * Inits the.
     * 
     * @param persistenceUnitName
     *            the persistence unit name
     * @return the entity manager
     * @throws Exception
     *             the exception
     */
    protected EntityManagerFactory createEntityManagerFactory(String persistenceUnitName)
    {
        return Persistence.createEntityManagerFactory(persistenceUnitName);

    }
}
