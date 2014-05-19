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
package com.impetus.client.cassandra.common;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.service.CassandraHost;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.utils.DefaultTimestampGenerator;
import com.impetus.kundera.utils.TimestampGenerator;

/**
 * Client factory interface to provide common method declaration across multiple
 * clients.
 * 
 * @author vivek.mishra
 * 
 */
public abstract class CassandraClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(CassandraClientFactory.class);

    /** The Timestamp Generator. */
    protected TimestampGenerator timestampGenerator = new DefaultTimestampGenerator();;

    /**
     * Add cassandra host.
     * 
     * @param host
     *            cassandra host configuration object
     * @return true, if it is added successfully to connection else false.
     */
    public abstract boolean addCassandraHost(CassandraHost host);

    /**
     * 
     * Initialize time stamp generator class.
     * 
     * @param externalProperty
     */
    protected void initializeTimestampGenerator(Map<String, Object> externalProperty)
    {
        String timestampGeneratorClass = (String) externalProperty.get(Constants.DEFAULT_TIMESTAMP_GENERATOR);
        if (timestampGeneratorClass == null)
        {
            timestampGeneratorClass = CassandraPropertyReader.csmd != null ? CassandraPropertyReader.csmd
                    .getDatastoreProperties().getProperty(Constants.DEFAULT_TIMESTAMP_GENERATOR, null) : null;
        }

        if (!StringUtils.isBlank(timestampGeneratorClass))
        {
            try
            {
                timestampGenerator = (TimestampGenerator) Class.forName(timestampGeneratorClass).newInstance();
            }
            catch (Exception ex)
            {
                logger.error("Error while initialzing timestamp generator class {}, caused by {}.",
                        timestampGeneratorClass, ex);
                throw new KunderaException(ex);
            }
        }
    }
}
