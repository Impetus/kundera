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

import java.util.Collection;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.rdbms.query.RDBMSEntityReader;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;

/**
 * A factory for creating RDBMSClient objects.
 * 
 * @author impadmin
 */
public class RDBMSClientFactory extends GenericClientFactory
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RDBMSClientFactory.class);

    /** The conf. */
    private Configuration conf;

    /** The sf. */
    private SessionFactory sf;

    private ServiceRegistry serviceRegistry;

    @Override
    public void destroy()
    {
        if (sf != null && !sf.isClosed())
        {            
            sf.close();
            sf = null;
        }
        indexManager.close();
        externalProperties = null;
    }

    @Override
    public void initialize(Map<String, Object> externalProperty)
    {
        reader = new RDBMSEntityReader();
        ((RDBMSEntityReader) reader).setFilter("where");
        setExternalProperties(externalProperty);
    }

    @Override
    protected Object createPoolOrConnection()
    {

        conf = new Configuration().addProperties(HibernateUtils.getProperties(getPersistenceUnit()));
        Collection<Class<?>> classes = ((MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                getPersistenceUnit())).getEntityNameToClassMap().values();
        // to keep hibernate happy! As in our case all scanned classes are not
        // meant for rdbms, so initally i have set depth to zero!
        conf.setProperty("hibernate.max_fetch_depth", "0");

        serviceRegistry = new ServiceRegistryBuilder().applySettings(conf.getProperties()).buildServiceRegistry();

        for (Class<?> c : classes)
        {
            conf.addAnnotatedClass(c);
        }
        sf = conf.buildSessionFactory(serviceRegistry);
        return sf;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new HibernateClient(getPersistenceUnit(), indexManager, reader, sf, externalProperties);
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        return null;
    }
}
