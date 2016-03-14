package com.impetus.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Table;

import org.apache.commons.lang.StringUtils;

import com.impetus.dao.PersistenceService;
import com.impetus.kundera.Constants;
import com.impetus.kundera.metadata.MetadataUtils;
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

        EntityMetadata metadata = new EntityMetadata(clazz);
        metadata.setPersistenceUnit(getPersistenceUnit());

        setSchemaAndPU(clazz, metadata);
        // metadata.setTableName("Employee");
        // metadata.setSchema("DAOtest");

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

    }

    private static void setSchemaAndPU(Class<?> clazz, EntityMetadata metadata)
    {
        Table table = clazz.getAnnotation(Table.class);
        if (table != null)
        {
            metadata.setTableName(!StringUtils.isBlank(table.name()) ? table.name() : clazz.getSimpleName());
            String schemaStr = table.schema();

            MetadataUtils.setSchemaAndPersistenceUnit(metadata, schemaStr,
                    em.getEntityManagerFactory().getProperties());
        }
        else{
            metadata.setTableName(clazz.getSimpleName());
            metadata.setSchema((String) em.getEntityManagerFactory().getProperties().get("kundera.keyspace"));
        }
        
        if (metadata.getPersistenceUnit() == null)
        {
            metadata.setPersistenceUnit(getPersistenceUnit());
        }
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
