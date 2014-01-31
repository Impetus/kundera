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
package com.impetus.kundera.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.query.CoreTestEntityReader;
import com.impetus.kundera.query.LuceneQuery;

/**
 * @author vivek.mishra
 * junit for {@link CoreTestClientNoGenerator}
 *
 */
public class CoreTestClientNoGenerator extends ClientBase implements Client<LuceneQuery>
{

    public CoreTestClientNoGenerator(IndexManager indexManager, String persistenceUnit, final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
        this.indexManager = indexManager;
        this.persistenceUnit = persistenceUnit;       
    }
    

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        DummySchema schema = DummyDatabase.INSTANCE.getSchema(entityMetadata.getSchema());        
       
        
        if(schema == null)
        {
            schema = new DummySchema();
            DummyTable table = new DummyTable();
            table.addRecord(id, entity);
            schema.addTable(entityMetadata.getTableName(), table);
            DummyDatabase.INSTANCE.addSchema(entityMetadata.getSchema(), schema);
        }
        else
        {
            DummyTable table = schema.getTable(entityMetadata.getTableName());
            if(table == null)
            {
                table = new DummyTable();
            }
            table.addRecord(id, entity);            
            schema.addTable(entityMetadata.getTableName(), table);            
        }         
    }
    
    @Override
    public Object find(Class entityClass, Object key)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        DummySchema schema = DummyDatabase.INSTANCE.getSchema(m.getSchema());
        if(schema == null) return null;
        
        DummyTable table = schema.getTable(m.getTableName());
        
        if(table == null) return null;
        
        return table.getRecord(key);        
    }
    
    @Override
    public void delete(Object entity, Object pKey)
    {
        if(entity == null) return;
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,  entity.getClass());
        DummySchema schema = DummyDatabase.INSTANCE.getSchema(m.getSchema());
        if(schema == null) return;
        DummyTable table = schema.getTable(m.getTableName());
        if(table == null) return;
        
        table.removeRecord(pKey);       
    }
    

    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        List results = new ArrayList();
        for(Object key : keys)
        {
            Object result = find(entityClass,key);
            if(result != null)
            {
                results.add(result);
            }
        }
        return results;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {

        return null;
    }

    @Override
    public void close()
    {

    }

    

    @Override
    public String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {

        return null;
    }

    @Override
    public EntityReader getReader()
    {
        return new CoreTestEntityReader(kunderaMetadata);
    }

    @Override
    public Class<LuceneQuery> getQueryImplementor()
    {
        return LuceneQuery.class;
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {

        return null;
    }


    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {

        return null;
    }


    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {

    }
}
