package com.impetus.client.couchbase;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.apache.commons.beanutils.ConvertUtils;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Class DefaultCouchbaseDataHandler.
 * 
 * @author devender.yadav
 * 
 */
public class DefaultCouchbaseDataHandler implements CouchbaseDataHandler
{
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.couchbase.CouchbaseDataHandler#getEntityFromDocument(
     * java.lang.Class, com.couchbase.client.java.document.json.JsonObject,
     * javax.persistence.metamodel.EntityType)
     */
    @Override
    public Object getEntityFromDocument(Class<?> entityClass, JsonObject obj, EntityType entityType)
    {
        Object entity = KunderaCoreUtils.createNewInstance(entityClass);
        Set<Attribute> attributes = entityType.getAttributes();
        Iterator<Attribute> iterator = attributes.iterator();
        iterateAndPopulateEntity(entity, obj, iterator);
        return entity;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.couchbase.CouchbaseDataHandler#getDocumentFromEntity(
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.Object,
     * com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata)
     */
    @Override
    public JsonDocument getDocumentFromEntity(EntityMetadata entityMetadata, Object entity,
            KunderaMetadata kunderaMetadata)
    {

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(entityMetadata.getPersistenceUnit());
        Class entityClazz = entityMetadata.getEntityClazz();
        EntityType entityType = metaModel.entity(entityClazz);
        Set<Attribute> attributes = entityType.getAttributes();
        Iterator<Attribute> iterator = attributes.iterator();
        JsonObject obj = iterateAndPopulateJsonObject(entity, iterator, entityMetadata.getTableName());
        Object id = PropertyAccessorHelper.getId(entity, entityMetadata);
        return JsonDocument.create(entityMetadata.getTableName() + CouchbaseConstants.ID_SEPARATOR + id.toString(),
                obj);
    }

    /**
     * Iterate and populate entity.
     *
     * @param entity
     *            the entity
     * @param obj
     *            the obj
     * @param iterator
     *            the iterator
     */
    private void iterateAndPopulateEntity(Object entity, JsonObject obj, Iterator<Attribute> iterator)
    {
        while (iterator.hasNext())
        {
            Attribute attribute = iterator.next();
            Field field = (Field) attribute.getJavaMember();
            String colName = ((AbstractAttribute) attribute).getJPAColumnName();

            if (!colName.equalsIgnoreCase(CouchbaseConstants.KUNDERA_ENTITY) && obj.get(colName) != null)
            {
                Object value = ConvertUtils.convert(obj.get(colName), field.getType());
                PropertyAccessorHelper.set(entity, field, value);
            }

        }
    }

    /**
     * Iterate and populate json object.
     *
     * @param entity
     *            the entity
     * @param iterator
     *            the iterator
     * @return the json object
     */
    private JsonObject iterateAndPopulateJsonObject(Object entity, Iterator<Attribute> iterator, String tableName)
    {
        JsonObject obj = JsonObject.create();
        while (iterator.hasNext())
        {
            Attribute attribute = iterator.next();
            Field field = (Field) attribute.getJavaMember();
            Object value = PropertyAccessorHelper.getObject(entity, field);

            obj.put(((AbstractAttribute) attribute).getJPAColumnName(), value);
        }
        obj.put(CouchbaseConstants.KUNDERA_ENTITY, tableName);
        return obj;
    }

}
