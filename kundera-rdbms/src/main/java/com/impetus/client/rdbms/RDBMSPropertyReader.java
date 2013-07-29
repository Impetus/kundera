/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;

/**
 * @author vivek.mishra
 * 
 * Implementation class to read external property configuration. Extends {@link AbstractPropertyReader}(future purpose), though none of method is supported.
 */
public class RDBMSPropertyReader extends AbstractPropertyReader
{

    /** The log instance. */
    private static final Logger log = LoggerFactory.getLogger(RDBMSPropertyReader.class);

    public RDBMSPropertyReader(Map externalProperties)
    {
        super(externalProperties);
    }

    /**
     * Reads property file which is given in persistence unit
     * 
     * @param pu
     */
    
    public Configuration load(String pu)
    {
        Configuration conf = new Configuration().addProperties(HibernateUtils.getProperties(pu));
        String propertyFileName = externalProperties != null ? (String)externalProperties.get(PersistenceProperties.KUNDERA_CLIENT_PROPERTY) :null;
        puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);
        if (propertyFileName == null)
        {
            propertyFileName = puMetadata != null ? puMetadata
                    .getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY) : null;
        }
        if (propertyFileName != null)
        {
            PropertyType fileType  = PropertyType.value(propertyFileName);
            
            switch (fileType)
            {
            case xml:
                conf.configure(propertyFileName);
                break;

            case properties:
                Properties props = new Properties();
                
                InputStream ioStream = puMetadata.getClassLoader().getResourceAsStream(propertyFileName);
                try
                {
                    props.load(ioStream);
                }
                catch (IOException e)
                {
                    log.error("Skipping as error occurred while loading property file {}, Cause by : {}.",propertyFileName,e);
                }
                
                conf.addProperties(props);
                break;

            default:
                log.error("Unsupported type{} for file{}, skipping load of properties.",fileType,propertyFileName);
                break;
            }
        }
        
        return conf;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.configure.AbstractPropertyReader#onXml(com.impetus.kundera.configure.ClientProperties)
     */
    @Override
    protected void onXml(ClientProperties cp)
    {
        throw new UnsupportedOperationException("Unsupported, support added with read() method");
    }

}
