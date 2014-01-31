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
package com.impetus.kundera.service;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.impetus.kundera.configure.ClientProperties.DataStore.Connection.Server;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * @author vivek.mishra
 *
 *  Dummy core host configuration class.
 */
public class CoreHostConfiguration extends HostConfiguration
{

    String hosts;
    String port;

    public CoreHostConfiguration(Map externalProperties, List<Server> servers, String persistenceUnit, KunderaMetadata kunderaMetadata)
    {
        super(externalProperties, servers, persistenceUnit, kunderaMetadata);
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.service.HostConfiguration#buildHosts(java.util.List, java.util.List)
     */
    @Override
    protected void buildHosts(List<Server> servers, List<Host> hostsList)
    {
     // do nothing

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.service.HostConfiguration#buildHosts(java.lang.String, java.lang.String, java.util.List)
     */
    @Override
    protected void buildHosts(String hosts, String portAsString, List<Host> hostsList)
    {
        this.hosts = hosts;
        this.port = portAsString;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.service.HostConfiguration#setConfig(com.impetus.kundera.service.Host, java.util.Properties, java.util.Map)
     */
    @Override
    protected void setConfig(Host host, Properties props, Map puProperties)
    {
        // do nothing

    }
    
    public void onValidation(final String host, final String port)
    {
        super.onValidation(host, port);
    }
}
