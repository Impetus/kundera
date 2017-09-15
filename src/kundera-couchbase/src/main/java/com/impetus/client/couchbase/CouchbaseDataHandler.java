package com.impetus.client.couchbase;

import javax.persistence.metamodel.EntityType;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Interface CouchbaseDataHandler.
 * 
 * @author devender.yadav
 */
public interface CouchbaseDataHandler
{

    /**
     * Gets the entity from document.
     *
     * @param entityClass
     *            the entity class
     * @param obj
     *            the obj
     * @param entityType
     *            the entity type
     * @return the entity from document
     */
    Object getEntityFromDocument(Class<?> entityClass, JsonObject obj, EntityType entityType);

    /**
     * Gets the document from entity.
     *
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param kunderaMetadata
     *            the kundera metadata
     * @return the document from entity
     */
    JsonDocument getDocumentFromEntity(EntityMetadata entityMetadata, Object entity,
            final KunderaMetadata kunderaMetadata);

}
