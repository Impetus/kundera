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
package com.impetus.kundera.configure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configurator to load and configure different configuration objects.
 * 
 * @author vivek.mishra
 * 
 */
public final class Configurator
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(Configurator.class);

    /**
     * holder for required configuration object. Do we really need to hold these
     * as a reference?
     */
    private List<Configuration> configurer = new ArrayList<Configuration>(2);

    /**
     * Constructor using fields.
     * 
     * @param persistenceUnits
     */
    public Configurator(String... persistenceUnits)
    {
        configurer.add(new PersistenceUnitConfiguration(persistenceUnits));
        configurer.add(new ClientFactoryConfiguraton(persistenceUnits));
        configurer.add(new MetamodelConfiguration(persistenceUnits));
        configurer.add(new SchemaConfiguration(persistenceUnits));
    }

    /**
     * Invokes on each configuration object.
     * @param properties TODO
     * 
     */
    public final void configure(Map properties)
    {
        for (Configuration conf : configurer)
        {
            logger.debug("Loading configuration for :" + conf.getClass().getSimpleName());
            conf.configure(null);
        }
    }
}
