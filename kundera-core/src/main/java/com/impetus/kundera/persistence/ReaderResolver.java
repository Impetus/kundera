/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.persistence;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.impetus.kundera.client.DBType;

/**
 * The Class ReaderResolver.
 * 
 * @author vivek.mishra
 */
final class ReaderResolver
{

    /** The reader col. */
    static Map<DBType, EntityReader> readerCol = new ConcurrentHashMap<DBType, EntityReader>();

    static
    {
        try
        {
            readerCol.put(DBType.CASSANDRA, (EntityReader) Class.forName(
                    "com.impetus.client.cassandra.query.CassandraEntityReader").newInstance());
            readerCol.put(DBType.RDBMS, (EntityReader) Class
                    .forName("com.impetus.client.rdbms.query.RDBMSEntityReader").newInstance());
        }
        catch (InstantiationException e)
        {
            throw new ReaderResolverException(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            throw new ReaderResolverException(e.getMessage());
        }
        catch (ClassNotFoundException e)
        {
            throw new ReaderResolverException(e.getMessage());
        }
    }

    /**
     * Gets the reader.
     * 
     * @param dbType
     *            the db type
     * @return the reader
     */
    static EntityReader getReader(DBType dbType)
    {
        return readerCol.get(dbType);
    }

}
