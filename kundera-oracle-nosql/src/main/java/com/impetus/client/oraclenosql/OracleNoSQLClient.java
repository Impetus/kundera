/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.oraclenosql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.lob.InputStreamVersion;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.oraclenosql.index.OracleNoSQLInvertedIndexer;
import com.impetus.client.oraclenosql.query.OracleNoSQLQuery;
import com.impetus.client.oraclenosql.query.OracleNoSQLQueryInterpreter;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Implementation of {@link Client} interface for Oracle NoSQL database
 * 
 * @author amresh.singh
 */
public class OracleNoSQLClient extends ClientBase implements Client<OracleNoSQLQuery>, Batcher
{

    /** LOB Constants */
    private static final int WRITE_TIMEOUT_SECONDS = 5;
    private static final Durability DURABILITY_DEFAULT = Durability.COMMIT_WRITE_NO_SYNC;
    private static final String LOB_SUFFIX = ".lob";

    /** The kvstore db. */
    private KVStore kvStore;

    private OracleNoSQLClientFactory factory;

    /** The reader. */
    private EntityReader reader;

    /** list of nodes for batch processing. */
    private List<Node> nodes = new ArrayList<Node>();

    /** batch size. */
    private int batchSize;

    /** The log. */
    private static Log log = LogFactory.getLog(OracleNoSQLClient.class);

    /**
     * Instantiates a new oracle no sqldb client.
     * 
     * @param kvStore
     *            the kv store
     * @param indexManager
     *            the index manager
     * @param reader
     *            the reader
     */
    OracleNoSQLClient(final OracleNoSQLClientFactory factory, EntityReader reader, IndexManager indexManager,
            final KVStore kvStore, Map<String, Object> puProperties, String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
        this.factory = factory;
        this.kvStore = kvStore;
        this.reader = reader;
        this.indexManager = indexManager;
        setBatchSize(persistenceUnit, puProperties);
    }

    @Override
    public Object find(Class entityClass, Object key)
    {
        return find(entityClass, key, null);
    }

    private Object find(Class entityClass, Object key, List<String> columnsToSelect)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(entityMetadata
                .getPersistenceUnit());

        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        if (log.isDebugEnabled())
        {
            log.debug("Fetching data from " + entityMetadata.getTableName() + " for PK " + key);
        }

        // Major Key components
        List<String> majorComponents = new ArrayList<String>();
        majorComponents.add(entityMetadata.getTableName());
        majorComponents.add(PropertyAccessorHelper.getString(key));

        Key majorKeyToFind = Key.createKey(majorComponents);

        Object entity = null;

        Map<String, Object> relationMap = null;
        if (entityMetadata.getRelationNames() != null && !entityMetadata.getRelationNames().isEmpty())
        {
            relationMap = new HashMap<String, Object>();
        }

        try
        {
            Iterator<KeyValueVersion> iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKeyToFind, null,
                    null);

            // If a record is found, instantiate entity and set ID value
            if (iterator.hasNext())
            {
                entity = entityMetadata.getEntityClazz().newInstance();
                PropertyAccessorHelper.setId(entity, entityMetadata, key);
            }

            // Populate non-ID attributes
            while (iterator.hasNext())
            {
                KeyValueVersion keyValueVersion = iterator.next();

                String minorKeyFirstPart = keyValueVersion.getKey().getMinorPath().get(0);                
                minorKeyFirstPart = removeLOBSuffix(minorKeyFirstPart);                
                String fieldName = entityMetadata.getFieldName(minorKeyFirstPart);

                if (fieldName != null)
                {
                    Field f = (Field) entityType.getAttribute(fieldName).getJavaMember();

                    if (metamodel.isEmbeddable(f.getType()))
                    {
                        // Populate embedded attribute
                        Class<?> embeddableClass = f.getType();
                        if (metamodel.isEmbeddable(embeddableClass))
                        {
                            String minorKeySecondPart = keyValueVersion.getKey().getMinorPath().get(1);
                            minorKeySecondPart = removeLOBSuffix(minorKeySecondPart);  
                            
                            Object embeddedObject = PropertyAccessorHelper.getObject(entity, f);
                            if (embeddedObject == null)
                            {
                                embeddedObject = embeddableClass.newInstance();
                                PropertyAccessorHelper.set(entity, f, embeddedObject);
                            }

                            EmbeddableType embeddableType = metamodel.embeddable(embeddableClass);
                            Attribute columnAttribute = embeddableType.getAttribute(minorKeySecondPart);
                            Field columnField = (Field) columnAttribute.getJavaMember();

                            if (columnField != null)
                            {
                                if(f.getType().isAssignableFrom(File.class))
                                {
                                    File lobFile = getLOBFile(keyValueVersion, minorKeySecondPart);
                                    PropertyAccessorHelper.set(embeddedObject, columnField, lobFile);
                                }
                                else
                                {
                                    byte[] value = keyValueVersion.getValue().getValue();
                                    PropertyAccessorHelper.set(embeddedObject, columnField, value);
                                }                
                                
                            }

                        }

                    }
                    else if (entityType.getAttribute(fieldName) != null)
                    {
                        
                        Value v = keyValueVersion.getValue();                        
                        if (f != null && entityMetadata.getRelation(f.getName()) == null)
                        {
                            if (columnsToSelect == null
                                    || columnsToSelect.isEmpty()
                                    || columnsToSelect
                                            .contains(((AbstractAttribute) entityType.getAttribute(fieldName))
                                                    .getJPAColumnName()))
                            {
                                if(f.getType().isAssignableFrom(File.class))
                                {
                                    File lobFile = getLOBFile(keyValueVersion, minorKeyFirstPart);
                                    PropertyAccessorHelper.set(entity, f, lobFile);

                                }
                                else
                                {
                                    // Populate non-embedded attribute                                    
                                    PropertyAccessorHelper.set(entity, f, v.getValue());
                                }                           
                                
                            }

                        }

                        else if (entityMetadata.getRelationNames() != null
                                && entityMetadata.getRelationNames().contains(minorKeyFirstPart))
                        {
                            Relation relation = entityMetadata.getRelation(f.getName());
                            EntityMetadata associationMetadata = KunderaMetadataManager.getEntityMetadata(relation
                                    .getTargetEntity());
                            relationMap.put(minorKeyFirstPart, PropertyAccessorHelper.getObject(associationMetadata
                                    .getIdAttribute().getBindableJavaType(), v.getValue()));
                        }

                    }
                }
            }
        }
        catch (Exception e)
        {
            log.error(e);
            throw new PersistenceException(e);
        }

        if (relationMap != null && !relationMap.isEmpty())
        {
            EnhanceEntity e = new EnhanceEntity(entity, key, relationMap);
            return e;
        }
        else
        {
            return entity;
        }
    }

    /**
     * @param keyValueVersion
     * @param fileName
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    private File getLOBFile(KeyValueVersion keyValueVersion, String fileName) throws FileNotFoundException,
            IOException
    {
        InputStreamVersion istreamVersion = kvStore.getLOB(keyValueVersion.getKey(), Consistency.NONE_REQUIRED, 5, TimeUnit.SECONDS);
        InputStream is = istreamVersion.getInputStream();
        
        File lobFile = new File(fileName);
        OutputStream os = new FileOutputStream(lobFile);
        int read = 0;
        byte[] bytes = new byte[1024];
        while ((read = is.read(bytes)) != -1)
        {
            os.write(bytes, 0, read);
        }
        return lobFile;
    }

    /**
     * @param minorKeyFirstPart
     * @return
     */
    private String removeLOBSuffix(String minorKeyFirstPart)
    {
        if(minorKeyFirstPart.endsWith(LOB_SUFFIX))
        {
            minorKeyFirstPart = minorKeyFirstPart.substring(0, minorKeyFirstPart.length() - LOB_SUFFIX.length());
        }
        return minorKeyFirstPart;
    }

    @Override
    public void close()
    {

    }

    @Override
    public void delete(Object entity, Object pKey)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
        String idString = PropertyAccessorHelper.getString(pKey);        
        
        Key key = Key.createKey(entityMetadata.getTableName());        
        KeyRange keyRange = new KeyRange(idString, true, idString, true);

        Iterator<KeyValueVersion> iterator = kvStore.storeIterator(Direction.UNORDERED, 0, key, keyRange, null);
        
        List<Operation> deleteOperations = new ArrayList<Operation>();
        
        while (iterator.hasNext())
        {
            KeyValueVersion keyValueVersion = iterator.next();
            
            List<String> minorKeysComponents = keyValueVersion.getKey().getMinorPath();
            if(minorKeysComponents.size() > 0 && minorKeysComponents.get(minorKeysComponents.size() - 1).endsWith(LOB_SUFFIX))
            {
                kvStore.deleteLOB(keyValueVersion.getKey(), DURABILITY_DEFAULT, WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            else
            {
                Operation op = kvStore.getOperationFactory().createDelete(keyValueVersion.getKey());                                        
                deleteOperations.add(op);                
            }
        
        }
        execute(deleteOperations);        
        //kvStore.multiDelete(Key.createKey(majorKeyComponent), null, null);     

        getIndexManager().remove(entityMetadata, entity, pKey.toString());
    }

    /**
     * On persist.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param relations
     *            the relations
     * @throws Exception
     *             the exception
     * @throws PropertyAccessException
     *             the property access exception
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        String schema = entityMetadata.getSchema(); // Irrelevant for this
                                                    // datastore
        String table = entityMetadata.getTableName();

        MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(entityMetadata
                .getPersistenceUnit());

        if (log.isDebugEnabled())
        {
            log.debug("Persisting data into " + schema + "." + table + " for " + id);
        }
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        List<Operation> persistOperations = new ArrayList<Operation>();

        // Major Key component
        List<String> majorKeyComponent = new ArrayList<String>();
        majorKeyComponent.add(table);        
        majorKeyComponent.add(PropertyAccessorHelper.getString(id)); //Major Keys are always String 

        // Iterate over all Non-ID attributes of this entity (ID is already part
        // of major key)
        Set<Attribute> attributes = entityType.getAttributes();

        for (Attribute attribute : attributes)
        {
            Field currentField = (Field) attribute.getJavaMember();
            Field idField = (Field) entityMetadata.getIdAttribute().getJavaMember();
            if (!currentField.equals(idField))
            {
                Class fieldJavaType = ((AbstractAttribute) attribute).getBindableJavaType();

                // If attribute is Embeddable, create minor keys for each
                // attribute it contains
                if (metamodel.isEmbeddable(fieldJavaType))
                {
                    Object embeddedObject = PropertyAccessorHelper.getObject(entity, (Field) attribute.getJavaMember());
                    if (embeddedObject != null)
                    {
                        if (attribute.isCollection())
                        {
                            // ElementCollection is not supported for
                            // OracleNoSQL as of now, ignore for now
                            log.warn("Attribute "
                                    + attribute.getName()
                                    + " will not be persistence because ElementCollection is not supported for OracleNoSQL as of now");
                        }
                        else
                        {
                            String embeddedColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
                            EmbeddableType embeddableType = metamodel.embeddable(fieldJavaType);
                            Set<Attribute> embeddableAttributes = embeddableType.getAttributes();

                            for (Attribute embeddableAttribute : embeddableAttributes)
                            {
                                Field f = (Field) embeddableAttribute.getJavaMember();

                                // Value
                                Object valueObj = PropertyAccessorHelper.getObject(embeddedObject, f);
                                if (valueObj != null)
                                {
                                    if (valueObj instanceof File)
                                    {
                                        List<String> minorKeyComponents = new ArrayList<String>();
                                        minorKeyComponents.add(embeddedColumnName);
                                        minorKeyComponents.add(((AbstractAttribute) embeddableAttribute).getJPAColumnName() + LOB_SUFFIX);
                                        
                                        // Key
                                        Key key = Key.createKey(majorKeyComponent, minorKeyComponents);
                                        File lobFile = (File) valueObj;
                                        saveLOBFile(key, lobFile);
                                    }
                                    else
                                    {

                                        List<String> minorKeyComponents = new ArrayList<String>();
                                        minorKeyComponents.add(embeddedColumnName);
                                        minorKeyComponents.add(((AbstractAttribute) embeddableAttribute)
                                                .getJPAColumnName());

                                        // Key
                                        Key key = Key.createKey(majorKeyComponent, minorKeyComponents);

                                        byte[] valueByteArray = PropertyAccessorHelper.getBytes(valueObj);
                                        Value value = Value.createValue(valueByteArray);

                                        Operation op = kvStore.getOperationFactory().createPut(key, value);                                        
                                        persistOperations.add(op);

                                    }
                                }
                            }
                        }
                    }
                }

                // All other non-embeddable agttributes (ignore associations, as
                // they will be store by separate call)
                else if (!attribute.isAssociation())
                {
                    Field field = (Field) attribute.getJavaMember();
                    String columnName = ((AbstractAttribute) attribute).getJPAColumnName();                   

                    // Value
                    Object valueObj = PropertyAccessorHelper.getObject(entity, field);
                    if (valueObj != null)
                    {
                        if(valueObj instanceof File)
                        {
                            // Key
                            Key key = Key.createKey(majorKeyComponent, columnName + LOB_SUFFIX);
                            File lobFile = (File) valueObj;                               
                            saveLOBFile(key, lobFile);
                        }
                        else
                        {
                            // Key
                            Key key = Key.createKey(majorKeyComponent, columnName);
                            
                            byte[] valueByteArray = PropertyAccessorHelper.getBytes(valueObj);
                            Value value = Value.createValue(valueByteArray);

                            Operation op = kvStore.getOperationFactory().createPut(key, value);
                            persistOperations.add(op);
                        }
                    }
                }
            }
        }

        // Iterate over relations
        if (rlHolders != null && !rlHolders.isEmpty())
        {
            for (RelationHolder rh : rlHolders)
            {
                String relationName = rh.getRelationName();
                Object valueObj = rh.getRelationValue();

                if (!StringUtils.isEmpty(relationName) && valueObj != null)
                {
                    // Key
                    Key key = Key.createKey(majorKeyComponent, relationName);

                    // Value
                    if (valueObj != null)
                    {
                        byte[] valueInBytes = PropertyAccessorHelper.getBytes(valueObj);
                        Value value = Value.createValue(valueInBytes);

                        Operation op = kvStore.getOperationFactory().createPut(key, value);
                        persistOperations.add(op);
                        // kvStore.put(key, value);
                    }
                }
            }
        }
        execute(persistOperations);
    }

    /**
     * Saves LOB file to Oracle KV Store
     * @param key
     * @param lobFile
     */
    private void saveLOBFile(Key key, File lobFile)
    {
        try
        {
            FileInputStream fis = new FileInputStream(lobFile);
            Version version = kvStore.putLOB(key, fis, DURABILITY_DEFAULT, WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        catch (FileNotFoundException e)
        {
            log.warn("Unable to find file " + lobFile + ". This is being omitted. Details:" + e.getMessage());
        } catch(IOException e)
        {
            log.warn("IOException while writing file " + lobFile + ". This is being omitted. Details:" + e.getMessage());
        }
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String joinTableName = joinTableData.getJoinTableName();
        String joinColumnName = joinTableData.getJoinColumnName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        /**
         * There will be two kinds of major keys 1.
         * /Join_Table_Name/Join_Column_Name/Primary_Key_On_Owning_Side 2.
         * /Join_Table_Name/Inverse_Join_Column_Name/Primary_Key_On_Other_Side
         * 
         * Minor keys for both will be list of Primary keys at the opposite
         * side, value will always be null
         */

        for (Object pk : joinTableRecords.keySet())
        {
            // Save Join Column ---> inverse Join Column mapping
            List<String> majorKeysForJoinColumn = new ArrayList<String>();

            majorKeysForJoinColumn.add(joinTableName);
            // majorKeysForJoinColumn.add(joinColumnName);
            majorKeysForJoinColumn.add(PropertyAccessorHelper.getString(pk));

            Set<Object> values = joinTableRecords.get(pk);
            List<String> minorKeysForJoinColumn = new ArrayList<String>();

            for (Object childId : values)
            {
                minorKeysForJoinColumn.add(PropertyAccessorHelper.getString(childId));

                // Save Invese join Column ---> Join Column mapping
                List<String> majorKeysForInvJoinColumn = new ArrayList<String>();
                majorKeysForInvJoinColumn.add(joinTableName);
                // majorKeysForInvJoinColumn.add(invJoinColumnName);
                majorKeysForInvJoinColumn.add(PropertyAccessorHelper.getString(childId));

                Key key = Key.createKey(majorKeysForInvJoinColumn, PropertyAccessorHelper.getString(pk));
                kvStore.put(key, Value.createValue(invJoinColumnName.getBytes()));
            }
            Key key = Key.createKey(majorKeysForJoinColumn, minorKeysForJoinColumn);
            kvStore.put(key, Value.createValue(joinColumnName.getBytes()));
        }
    }

    private void execute(List<Operation> batch)
    {
        try
        {
            kvStore.execute(batch);
        }
        catch (DurabilityException e)
        {
            log.error(e);
            throw new PersistenceException("Error while Persisting data using batch", e);
        }
        catch (OperationExecutionException e)
        {
            log.error(e);
            throw new PersistenceException("Error while Persisting data using batch", e);
        }
        catch (FaultException e)
        {
            log.error(e);
            throw new PersistenceException("Error while Persisting data using batch", e);
        }
        finally
        {
            batch.clear();
        }
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys)
    {
        return findAll(entityClass, new String[0], keys);
    }

    private <E> List<E> findAll(Class<E> entityClass, String[] selectColumns, Object... keys)
    {
        List<E> results = new ArrayList<E>();

        for (Object key : keys)
        {
            results.add((E) find(entityClass, key, Arrays.asList(selectColumns)));
        }

        return results;
    }

    public <E> List<E> executeQuery(Class<E> entityClass, OracleNoSQLQueryInterpreter interpreter)
    {
        List<E> results = new ArrayList<E>();

        Set<Object> primaryKeys = null;

        if (!interpreter.getClauseQueue().isEmpty()) // Select all query
        {
            // Select Query with where clause (requires search within inverted
            // index)
            primaryKeys = ((OracleNoSQLInvertedIndexer) getIndexManager().getIndexer()).executeQuery(interpreter,
                    entityClass);

        }
        else
        {
            EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClass);

            ArrayList<String> majorComponents = new ArrayList<String>();
            majorComponents.add(m.getTableName());

            Key key = Key.createKey(majorComponents);

            Iterator<KeyValueVersion> iterator = kvStore.storeIterator(Direction.UNORDERED, 0, key, null, null);

            Set<Object> keySet = new HashSet<Object>();

            while (iterator.hasNext())
            {
                KeyValueVersion keyValueVersion = iterator.next();

                String majorKeySecondPart = keyValueVersion.getKey().getMajorPath().get(1);
                keySet.add(majorKeySecondPart);
            }

            primaryKeys = keySet;
        }

        results = findAll(entityClass, interpreter.getSelectColumns(), primaryKeys.toArray());

        return results;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        return null;
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {
        List<E> foreignKeys = new ArrayList<E>();

        // Major Key components
        List<String> majorComponents = new ArrayList<String>();
        majorComponents.add(tableName);
        // majorComponents.add(pKeyColumnName);
        majorComponents.add(PropertyAccessorHelper.getString(pKeyColumnValue));
        Key majorKeyToFind = Key.createKey(majorComponents);

        Iterator<KeyValueVersion> iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKeyToFind, null, null);

        try
        {
            while (iterator.hasNext())
            {
                KeyValueVersion keyValueVersion = iterator.next();

                String value = new String(keyValueVersion.getValue().getValue(), "UTF-8");
                if (value != null && value.equals(pKeyColumnName))
                {
                    Iterator<String> minorKeyIterator = keyValueVersion.getKey().getMinorPath().iterator();

                    while (minorKeyIterator.hasNext())
                    {
                        String minorKey = minorKeyIterator.next();
                        Object foreignKey = PropertyAccessorHelper.fromSourceToTargetClass(columnJavaType,
                                String.class, minorKey);
                        foreignKeys.add((E) foreignKey);
                    }
                }
            }
        }
        catch (UnsupportedEncodingException e)
        {
            log.error(e.getMessage());
            throw new PersistenceException(e);
        }
        return foreignKeys;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        String valueAsStr = PropertyAccessorHelper.getString(columnValue);
        Set<Object> results = new HashSet<Object>();

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClazz);
        // Major Key components
        List<String> majorComponents = new ArrayList<String>();
        majorComponents.add(tableName);
        // majorComponents.add(columnName);
        majorComponents.add(PropertyAccessorHelper.getString(valueAsStr));
        Key majorKeyToFind = Key.createKey(majorComponents);

        Iterator<KeyValueVersion> iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKeyToFind, null, null);
        try
        {
            while (iterator.hasNext())
            {
                KeyValueVersion keyValueVersion = iterator.next();
                String value = new String(keyValueVersion.getValue().getValue(), "UTF-8");

                if (value != null && value.equals(columnName))
                {
                    Iterator<String> minorKeyIterator = keyValueVersion.getKey().getMinorPath().iterator();

                    while (minorKeyIterator.hasNext())
                    {
                        String minorKey = minorKeyIterator.next();
                        Object primaryKey = PropertyAccessorHelper.fromSourceToTargetClass(metadata.getIdAttribute()
                                .getJavaType(), String.class, minorKey);
                        results.add(primaryKey);
                    }
                }
            }
        }
        catch (UnsupportedEncodingException e)
        {
            log.error(e.getMessage());
            throw new PersistenceException(e);
        }

        if (results != null && !results.isEmpty())
        {
            return results.toArray(new Object[0]);
        }
        return null;
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        List<String> majorKeyComponent = new ArrayList<String>();
        majorKeyComponent.add(tableName);
        majorKeyComponent.add(PropertyAccessorHelper.getString(columnValue));
        Key majorKey = Key.createKey(majorKeyComponent);
        boolean deleteApplicableOnMajorKey = false;

        // Store minor keys in an array before deleting
        List<String> minorKeys = new ArrayList<String>();
        Iterator<KeyValueVersion> iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKey, null, null);
        try
        {
            while (iterator.hasNext())
            {
                KeyValueVersion keyValueVersion = iterator.next();

                String value = new String(keyValueVersion.getValue().getValue(), "UTF-8");
                if (value != null && value.equals(columnName))
                {
                    deleteApplicableOnMajorKey = true;

                    Iterator<String> minorKeyIterator = keyValueVersion.getKey().getMinorPath().iterator();

                    while (minorKeyIterator.hasNext())
                    {
                        String minorKey = minorKeyIterator.next();
                        minorKeys.add(minorKey);
                    }
                }

            }
        }
        catch (UnsupportedEncodingException e)
        {
            log.error(e.getMessage());
            throw new PersistenceException(e);
        }

        if (deleteApplicableOnMajorKey)
        {
            // Delete This columnValue as major key
            kvStore.multiDelete(majorKey, null, null);

            // Delete all minor keys that contain this columnValue
            for (String key : minorKeys)
            {
                List<String> majorKeys = new ArrayList<String>();
                majorKeys.add(tableName);
                majorKeys.add(PropertyAccessorHelper.getString(key));
                Key majorAndMinorKeys = Key.createKey(majorKeys, PropertyAccessorHelper.getString(columnValue));
                kvStore.multiDelete(majorAndMinorKeys, null, null);
            }
        }

    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        return null;
    }

    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    @Override
    public Class<OracleNoSQLQuery> getQueryImplementor()
    {
        return OracleNoSQLQuery.class;
    }

    @Override
    public void addBatch(Node node)
    {
        if (node != null)
        {
            nodes.add(node);
        }
        onBatchLimit();
    }

    @Override
    public int executeBatch()
    {
        for (Node node : nodes)
        {
            if (node.isDirty())
            {
                // delete can not be executed in batch
                if (node.isInState(RemovedState.class))
                {
                    delete(node.getData(), node.getEntityId());
                }
                else
                {
                    EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(node.getDataClass());
                    List<RelationHolder> relationHolders = getRelationHolders(node);
                    onPersist(metadata, node.getData(), node.getEntityId(), relationHolders);
                }
            }
        }
        return nodes.size();
    }

    @Override
    public int getBatchSize()
    {
        return batchSize;
    }

    @Override
    public void clear()
    {
        if (nodes != null)
        {
            nodes.clear();
            nodes = null;
            nodes = new ArrayList<Node>();
        }

    }

    /**
     * Check on batch limit.
     */
    private void onBatchLimit()
    {
        if (batchSize > 0 && batchSize == nodes.size())
        {
            executeBatch();
            nodes.clear();
        }
    }

    /**
     * @param persistenceUnit
     * @param puProperties
     */
    private void setBatchSize(String persistenceUnit, Map<String, Object> puProperties)
    {
        String batch_Size = null;
        if (puProperties != null)
        {
            batch_Size = puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE)
                    : null;
            if (batch_Size != null)
            {
                batchSize = Integer.valueOf(batch_Size);
                if (batchSize == 0)
                {
                    throw new IllegalArgumentException("kundera.batch.size property must be numeric and > 0");
                }
            }
        }
        else if (batch_Size == null)
        {
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
            batchSize = puMetadata.getBatchSize();
        }
    }

}
