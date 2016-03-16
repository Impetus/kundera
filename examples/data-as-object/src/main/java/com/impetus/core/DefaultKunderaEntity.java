package com.impetus.core;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Table;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.processor.IndexProcessor;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;

public class DefaultKunderaEntity<T, K> implements KunderaEntity<T, K>
{
    private static EntityManagerFactory emf;

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

        new TableProcessor(em.getEntityManagerFactory().getProperties(),
                ((EntityManagerFactoryImpl) em.getEntityManagerFactory()).getKunderaMetadataInstance()).process(clazz,
                        metadata);

        KunderaMetadata kunderaMetadata = ((EntityManagerFactoryImpl) em.getEntityManagerFactory())
                .getKunderaMetadataInstance();

        new IndexProcessor(kunderaMetadata).process(clazz, metadata);

        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();

        ((MetamodelImpl) em.getMetamodel()).addEntityMetadata(clazz, metadata);
        ((MetamodelImpl) em.getMetamodel()).addEntityNameToClassMapping(clazz.getSimpleName(), clazz);
        appMetadata.getMetamodelMap().put(getPersistenceUnit(), em.getMetamodel());

        Map<String, List<String>> clazzToPuMap = new HashMap<String, List<String>>();
        List<String> persistenceUnits = new ArrayList<String>();
        persistenceUnits.add(getPersistenceUnit());
        clazzToPuMap.put(clazz.getName(), persistenceUnits);
        appMetadata.setClazzToPuMap(clazzToPuMap);
        new SchemaConfiguration(em.getEntityManagerFactory().getProperties(), kunderaMetadata, getPersistenceUnit())
                .configure();

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
        else
        {
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
            em = PersistenceService.getEM(emf, propertiesPath, clazz.getName());
        }
        onBind(clazz);
    }

    public static synchronized void unbind()
    {
        if (emf != null && emf.isOpen())
        {
            emf.close();
            emf = null;

        }
        if (em != null && em.isOpen())
        {
            em.close();
            em = null;
        }
    }

    public final List leftJoin(Class clazz, String joinColumn, String... columnTobeFetched)
    {
        List<T> finalResult = new ArrayList();
        List<T> leftTable = em.createQuery("Select p from " + this.getClass().getSimpleName() + " p").getResultList();
        EntityType leftEntity = ((MetamodelImpl) em.getMetamodel()).entity(this.getClass());
        Attribute attribute = leftEntity.getAttribute(joinColumn);
        Field field = (Field) attribute.getJavaMember();

        for (T obj : leftTable)
        {
            List rightTable = em
                    .createQuery(
                            "Select p from " + clazz.getSimpleName() + " p where p." + joinColumn + " = :columnValue")
                    .setParameter("columnValue", PropertyAccessorHelper.getObject(obj, field)).getResultList();
            if (!rightTable.isEmpty())
            {
                finalResult.add(obj);
            }
        }
        return finalResult;
    }

    public List<T> query(String query)
    {
        return em.createQuery(query).getResultList();
    }

    public List<T> query(String query, QueryType type)
    {
        switch (type)
        {
        case JPQL:
            return query(query);

        case NATIVE:
            return nativeQuery(query);

        default:
            throw new KunderaException("invalid query type");
        }
    }

    private List<T> nativeQuery(String query)
    {
        return em.createNativeQuery(query).getResultList();
    }

}
