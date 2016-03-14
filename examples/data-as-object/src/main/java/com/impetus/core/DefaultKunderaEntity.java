package com.impetus.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;

import com.impetus.dao.PersistenceService;
import com.impetus.kundera.Constants;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

public class DefaultKunderaEntity<T, K> implements KunderaEntity<T, K>
{
    private static EntityManager em;

    public final T find(K key)
    {

        return (T) em.find(this.getClass(), key);
    }

    private static void onBind(Class clazz)
    {

        EntityMetadata metadata = new EntityMetadata(clazz/* this.getClass() */);
        metadata.setPersistenceUnit(getPersistenceUnit());
        metadata.setTableName("Employee");
        metadata.setSchema("DAOtest");
        new TableProcessor(em.getEntityManagerFactory().getProperties(),
                ((EntityManagerFactoryImpl) em.getEntityManagerFactory()).getKunderaMetadataInstance()).process(clazz,
                        metadata);

        KunderaMetadata kunderaMetadata = ((EntityManagerFactoryImpl) em.getEntityManagerFactory())
                .getKunderaMetadataInstance();

        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
        ((MetamodelImpl) em.getMetamodel()).addEntityMetadata(clazz, metadata);
        appMetadata.getMetamodelMap().put(getPersistenceUnit(), em.getMetamodel());
        Map<String, List<String>> clazzToPuMap = new HashMap<String, List<String>>();

        List<String> persistenceUnits = new ArrayList<String>();
        persistenceUnits.add(getPersistenceUnit());
        clazzToPuMap.put(clazz.getName(), persistenceUnits);
        appMetadata.setClazzToPuMap(clazzToPuMap);
        /*
         * new
         * MetamodelConfiguration(em.getEntityManagerFactory().getProperties(),
         * ((EntityManagerFactoryImpl)em.getEntityManagerFactory()).
         * getKunderaMetadataInstance(), getPersistenceUnit()).configure();
         * 
         * // KunderaMetadataManager.getMetamodel(((EntityManagerFactoryImpl)em.
         * getEntityManagerFactory()).getKunderaMetadataInstance(),
         * getPersistenceUnit()).addEntityMetadata(this.getClass(), metadata);
         * ((MetamodelImpl)em.getMetamodel()).addEntityMetadata(this.getClass(),
         * metadata);
         */
    }

    private static String getPersistenceUnit()
    {
        return (String) em.getEntityManagerFactory().getProperties().get(Constants.PERSISTENCE_UNIT_NAME);
    }

    public final void save()
    {
        em.persist(this);
    }

    public final void update()
    {
        em.merge(this);
    }

    public final void delete()
    {
        em.remove(this);
    }

    public static synchronized void bind(String propertiesPath, Class clazz) throws BindingException
    {
        if (em == null)
        {
            em = PersistenceService.getEM(propertiesPath);
        }
        onBind(clazz);
    }

    public static synchronized void unbind()
    {
        em.getEntityManagerFactory().close();
        em.close();
    }
}
