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
package com.impetus.client.rdbms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.rdbms.query.RDBMSEntityReader;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;

/**
 * A factory for creating RDBMSClient objects.
 * 
 * @author impadmin
 */
public class RDBMSClientFactory extends GenericClientFactory
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RDBMSClientFactory.class);

    @Override
    public void destroy()
    {
        indexManager.close();
    }

    @Override
    public void initialize()
    {        
        reader = new RDBMSEntityReader();
        ((RDBMSEntityReader) reader).setFilter("where");
    }

    @Override
    protected Object createPoolOrConnection()
    {

        // Do nothing.
        return null;
    }


    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new HibernateClient(getPersistenceUnit(), indexManager, reader);
    }


    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    public SchemaManager getSchemaManager()
    {
        return null;
    }
}
