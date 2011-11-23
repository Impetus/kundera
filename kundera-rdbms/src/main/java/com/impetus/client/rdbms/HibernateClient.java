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
package com.impetus.client.rdbms;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import org.apache.commons.lang.NotImplementedException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.ejb.HibernatePersistence;

//import com.impetus.client.Player;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * @author vivek.mishra
 */
public class HibernateClient implements Client
{
    private String persistenceUnit;
    private Configuration conf;
//    private EntityManagerFactory emf;
    private SessionFactory sf; 
    public HibernateClient(final String persistenceUnit)
    {
         conf = new Configuration().addProperties(HibernateUtils.getProperties(persistenceUnit));
        Collection<Class<?>> classes = ((MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().
                getMetamodel(persistenceUnit)).getEntityNameToClassMap().values();
        for(Class<?> c: classes)
        {
            System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<"+c.getCanonicalName());
            conf.addAnnotatedClass(c);
        }
        sf = conf.buildSessionFactory();
        
        //TODO . once we clear this persistenceUnit stuff we need to simply modify this to have a properties or even pass an EMF! 
        this.persistenceUnit = persistenceUnit;
//        HibernatePersistence p = new HibernatePersistence();
//        emf = p.createEntityManagerFactory(persistenceUnit, HibernateUtils.getProperties(persistenceUnit));
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#persist(com.impetus.kundera.proxy.EnhancedEntity)
     */
    @Override
    public void persist(EnhancedEntity enhanceEntity) throws Exception
    {
     //   conf.addAnnotatedClass(Player.class);
        Session s = null;
        if(sf.isClosed())
        {
            s = sf.openSession();
        } else
        {
            s = sf.getCurrentSession();
        }
        Transaction tx = s.beginTransaction();
//       EntityManager em = emf.createEntityManager();
       s.persist(enhanceEntity.getEntity());
       tx.commit();
//       s.c
//       s.close();
    }



    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#getIndexManager()
     */
    @Override
    public IndexManager getIndexManager()
    {
        
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#getPersistenceUnit()
     */
    @Override
    public String getPersistenceUnit()
    {
        
        return persistenceUnit;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#loadData(javax.persistence.Query)
     */
    @Override
    public <E> List<E> loadData(Query arg0) throws Exception
    {
        
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#setPersistenceUnit(java.lang.String)
     */
    @Override
    @Deprecated
    public void setPersistenceUnit(String arg0)
    {
//        throw new NotImplementedException("This support is already depricated");

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#delete(com.impetus.kundera.proxy.EnhancedEntity)
     */
    @Override
    public void delete(EnhancedEntity arg0) throws Exception
    {
        

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, java.lang.String)
     */
    @Override
    public <E> E find(Class<E> arg0, String arg1) throws Exception
    {
        
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, java.lang.String[])
     */
    @Override
    public <E> List<E> find(Class<E> arg0, String... arg1) throws Exception
    {
        
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> arg0, Map<String, String> arg1) throws Exception
    {
        
        return null;
    }

}
