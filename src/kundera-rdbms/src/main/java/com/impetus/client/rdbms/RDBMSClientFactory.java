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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.rdbms.query.RDBMSEntityReader;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;

/**
 * A factory for creating RDBMSClient objects.
 * 
 * @author vivek.mishra
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
        unload();

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
        reader = new RDBMSEntityReader(kunderaMetadata);
        ((RDBMSEntityReader) reader).setFilter("where");
        setExternalProperties(externalProperty);
    }

    @Override
    protected Object createPoolOrConnection()
    {

        getConfigurationObject();
        Set<String> pus = kunderaMetadata.getApplicationMetadata().getMetamodelMap().keySet();

        Map<String, Collection<Class<?>>> classes = new HashMap<String, Collection<Class<?>>>();

        for (String pu : pus)
        {
            classes.put(pu,
                    /* Collection<Class<?>> classes = */((MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                            .getMetamodel(pu)).getEntityNameToClassMap().values());
        }
        // to keep hibernate happy! As in our case all scanned classes are not
        // meant for rdbms, so initally i have set depth to zero!
        conf.setProperty("hibernate.max_fetch_depth", "0");

        if (externalProperties != null && !externalProperties.isEmpty())
        {
            for (String key : externalProperties.keySet())
            {
                Object value = externalProperties.get(key);
                if (value instanceof String)
                {
                    conf.setProperty(key, (String) value);
                }
            }
        }

        serviceRegistry = new StandardServiceRegistryBuilder().applySettings(conf.getProperties()).build();

        Iterator<Collection<Class<?>>> iter = classes.values().iterator();

        while (iter.hasNext())
        {
            for (Class<?> c : iter.next())
            {
                conf.addAnnotatedClass(c);
            }
        }
        sf = conf.buildSessionFactory(serviceRegistry);

        String schemaProperty = conf.getProperty("hibernate.hbm2ddl.auto");
        if (schemaProperty != null && (schemaProperty.equals("create") || schemaProperty.equals("create-drop")))
        {
            synchronized (sf)
            {
                for (String pu : pus)
                {
                    StatelessSession session = sf.openStatelessSession();
                    if (!pu.equals(getPersistenceUnit()))
                    {
                        Collection<Class<?>> collection = classes.get(pu);
                        for (Class clazz : collection)
                        {
                            EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz);
                            try
                            {
                                session.createSQLQuery("Drop table " + metadata.getTableName()).executeUpdate();
                            }
                            catch (Exception e)
                            {
                                // ignore such drops.
                            }
                        }
                    }

                }
            }
        }

        return sf;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new HibernateClient(getPersistenceUnit(), indexManager, reader, this, externalProperties,
                clientMetadata, kunderaMetadata);
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

    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }

    /**
     * Returns configuration object.
     */
    private void getConfigurationObject()
    {
        RDBMSPropertyReader reader = new RDBMSPropertyReader(externalProperties, kunderaMetadata
                .getApplicationMetadata().getPersistenceUnitMetadata(getPersistenceUnit()));
        this.conf = reader.load(getPersistenceUnit());
    }

    Session getSession()
    {
        if (sf != null)
        {
            return sf.openSession();
        }
        throw new PersistenceException("Session factory is not initialized");
    }

    StatelessSession getStatelessSession()
    {
        if (sf != null)
        {
            return sf.openStatelessSession();
        }
        throw new PersistenceException("Session factory is not initialized");
    }
}
