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
package com.impetus.kundera.configure;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.persistence.DiscriminatorColumn;
import javax.persistence.Lob;
import javax.persistence.OrderBy;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.metamodel.SingularAttribute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.configure.schema.CollectionColumnInfo;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.EmbeddedColumnInfo;
import com.impetus.kundera.configure.schema.IndexInfo;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.ClientFactory;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
import com.impetus.kundera.metadata.model.IdDiscriptor;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.PropertyIndex;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.metadata.processor.IndexProcessor;
import com.impetus.kundera.metadata.validator.EntityValidator;
import com.impetus.kundera.metadata.validator.EntityValidatorImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * Schema configuration implementation to support ddl_schema_creation
 * functionality. e.g. kundera_ddl_auto_prepare
 * (create,create-drop,validate,update)
 * 
 * @author Kuldeep.Kumar
 * 
 */
public class SchemaConfiguration extends AbstractSchemaConfiguration implements Configuration
{
    /** The log. */
    private static Logger log = LoggerFactory.getLogger(SchemaConfiguration.class);

    /**
     * pu to schema metadata map .
     */
    private Map<String, List<TableInfo>> puToSchemaMetadata = new HashMap<String, List<TableInfo>>();

    /**
     * Constructor using persistence units as parameter.
     * 
     * @param persistenceUnits
     *            persistence units.
     */
    public SchemaConfiguration(Map externalProperties, final KunderaMetadata metadata, String... persistenceUnits)
    {
        super(persistenceUnits, externalProperties, metadata);
    }

    @Override
    /**
     * configure method responsible for creating pu to schema metadata map for
     * each entity in class path.
     * 
     */
    public void configure()
    {
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();

        // TODO, FIXME: Refactoring is required.
        for (String persistenceUnit : persistenceUnits)
        {
            EntityValidator validator = new EntityValidatorImpl(
                    KunderaCoreUtils.getExternalProperties(persistenceUnit, externalPropertyMap, persistenceUnits));

            log.info("Configuring schema export for : " + persistenceUnit);
            List<TableInfo> tableInfos = getSchemaInfo(persistenceUnit);

            Map<String, EntityMetadata> entityMetadataMap = getEntityMetadataCol(appMetadata, persistenceUnit);

            PersistenceUnitMetadata puMetadata = appMetadata.getPersistenceUnitMetadata(persistenceUnit);

            // Iterate each entity metadata.
            for (EntityMetadata entityMetadata : entityMetadataMap.values())
            {
                // get entity metadata(table info as well as columns)
                // if table info exists, get it from map.
                boolean found = false;

                Type type = entityMetadata.getType();
                Class idClassName = entityMetadata.getIdAttribute() != null
                        ? entityMetadata.getIdAttribute().getJavaType() : null;

                String idName = entityMetadata.getIdAttribute() != null
                        ? ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName() : null;

                TableInfo tableInfo = new TableInfo(entityMetadata.getTableName(), type.name(), idClassName, idName);

                // check for tableInfos not empty and contains the present
                // tableInfo.
                if (!tableInfos.isEmpty() && tableInfos.contains(tableInfo))
                {
                    found = true;
                    int idx = tableInfos.indexOf(tableInfo);
                    tableInfo = tableInfos.get(idx);
                    addColumnToTableInfo(entityMetadata, type, tableInfo);
                }
                else
                {
                    addColumnToTableInfo(entityMetadata, type, tableInfo);
                }

                List<Relation> relations = entityMetadata.getRelations();

                parseRelations(persistenceUnit, tableInfos, entityMetadata, tableInfo, relations);

                if (!found)
                {
                    tableInfos.add(tableInfo);
                }
                // Add table for GeneratedValue if opted TableStrategy
                addTableGenerator(appMetadata, persistenceUnit, tableInfos, entityMetadata);

                // Validating entity against counter column family.
                validator.validateEntity(entityMetadata.getEntityClazz(), kunderaMetadata);
            }

            puToSchemaMetadata.put(persistenceUnit, tableInfos);

        }

        // Need to iterate, as in case of non unary relations
        for (String persistenceUnit : persistenceUnits)
        {
            PersistenceUnitMetadata puMetadata = appMetadata.getPersistenceUnitMetadata(persistenceUnit);

            Map externalPuMap = externalPropertyMap;

            // in case of polyglot.
            if (persistenceUnits.length > 1 && externalPropertyMap != null)
            {
                externalPuMap = (Map) externalPropertyMap.get(persistenceUnit);
            }
            if (externalPuMap != null && externalPuMap.get(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE) != null
                    || puMetadata.getProperty(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE) != null)
            {
                SchemaManager schemaManager = getSchemaManagerForPu(persistenceUnit);

                if (schemaManager != null)
                {
                    schemaManager.exportSchema(persistenceUnit, puToSchemaMetadata.get(persistenceUnit));
                }
            }
        }
    }

    /**
     * Return schema manager for pu.
     * 
     * @param persistenceUnit
     * @return
     */
    private SchemaManager getSchemaManagerForPu(final String persistenceUnit)
    {
        SchemaManager schemaManager = null;
        Map<String, Object> externalProperties = KunderaCoreUtils.getExternalProperties(persistenceUnit,
                externalPropertyMap, persistenceUnits);
        if (getSchemaProperty(persistenceUnit, externalProperties) != null
                && !getSchemaProperty(persistenceUnit, externalProperties).isEmpty())
        {
            ClientFactory clientFactory = ClientResolver.getClientFactory(persistenceUnit);
            schemaManager = clientFactory != null ? clientFactory.getSchemaManager(externalProperties) : null;
        }
        return schemaManager;
    }

    /**
     * Add tableGenerator to table info.
     * 
     * @param appMetadata
     * @param persistenceUnit
     * @param tableInfos
     * @param entityMetadata
     * @param isCompositeId
     */
    private void addTableGenerator(ApplicationMetadata appMetadata, String persistenceUnit, List<TableInfo> tableInfos,
            EntityMetadata entityMetadata)
    {
        Metamodel metamodel = appMetadata.getMetamodel(persistenceUnit);
        IdDiscriptor keyValue = ((MetamodelImpl) metamodel).getKeyValue(entityMetadata.getEntityClazz().getName());
        if (keyValue != null && keyValue.getTableDiscriptor() != null)
        {
            TableInfo tableGeneratorDiscriptor = new TableInfo(keyValue.getTableDiscriptor().getTable(),
                    "CounterColumnType", String.class, keyValue.getTableDiscriptor().getPkColumnName());
            if (!tableInfos.contains(tableGeneratorDiscriptor))
            {
                tableGeneratorDiscriptor.addColumnInfo(getJoinColumn(tableGeneratorDiscriptor,
                        keyValue.getTableDiscriptor().getValueColumnName(), Long.class));
                tableInfos.add(tableGeneratorDiscriptor);
            }
        }
    }

    /**
     * parse the relations of entites .
     * 
     * @param persistenceUnit
     * @param tableInfos
     * @param entityMetadata
     * @param tableInfo
     * @param relations
     */
    private void parseRelations(String persistenceUnit, List<TableInfo> tableInfos, EntityMetadata entityMetadata,
            TableInfo tableInfo, List<Relation> relations)
    {
        for (Relation relation : relations)
        {
            if (relation != null)
            {
                Class entityClass = relation.getTargetEntity();
                EntityMetadata targetEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                        entityClass);
                if (targetEntityMetadata == null)
                {
                    log.error("Persistence unit for class : " + entityClass + " is not loaded");
                    throw new SchemaGenerationException(
                            "Persistence unit for class : " + entityClass + " is not loaded");
                }
                ForeignKey relationType = relation.getType();

                // if relation type is one to many or join by primary key
                if (targetEntityMetadata != null && relationType.equals(ForeignKey.ONE_TO_MANY)
                        && relation.getJoinColumnName(kunderaMetadata) != null)
                {
                    // if self association
                    if (targetEntityMetadata.equals(entityMetadata))
                    {
                        tableInfo.addColumnInfo(getJoinColumn(tableInfo, relation.getJoinColumnName(kunderaMetadata),
                                entityMetadata.getIdAttribute().getJavaType()));
                    }
                    else
                    {
                        String pu = targetEntityMetadata.getPersistenceUnit();
                        Type targetEntityType = targetEntityMetadata.getType();
                        Class idClass = targetEntityMetadata.getIdAttribute().getJavaType();
                        String idName = ((AbstractAttribute) targetEntityMetadata.getIdAttribute()).getJPAColumnName();
                        TableInfo targetTableInfo = new TableInfo(targetEntityMetadata.getTableName(),
                                targetEntityType.name(), idClass, idName);

                        // In case of different persistence unit. case for poly
                        // glot
                        // persistence.
                        if (!pu.equals(persistenceUnit))
                        {
                            List<TableInfo> targetTableInfos = getSchemaInfo(pu);

                            addJoinColumnToInfo(relation.getJoinColumnName(kunderaMetadata), targetTableInfo,
                                    targetTableInfos, entityMetadata);

                            // add for newly discovered persistence unit.
                            puToSchemaMetadata.put(pu, targetTableInfos);
                        }
                        else
                        {
                            addJoinColumnToInfo(relation.getJoinColumnName(kunderaMetadata), targetTableInfo,
                                    tableInfos, entityMetadata);
                        }
                    }
                }
                // if relation type is one to one or many to one.
                else if (relation.isUnary() && relation.getJoinColumnName(kunderaMetadata) != null)
                {
                    if (!relation.isJoinedByPrimaryKey())
                    {
                        tableInfo.addColumnInfo(getJoinColumn(tableInfo, relation.getJoinColumnName(kunderaMetadata),
                                targetEntityMetadata.getIdAttribute().getJavaType()));
                    }
                }
                // if relation type is many to many and relation via join table.
                else if ((relationType.equals(ForeignKey.MANY_TO_MANY)) && (entityMetadata.isRelationViaJoinTable()))
                {
                    JoinTableMetadata joinTableMetadata = relation.getJoinTableMetadata();
                    String joinTableName = joinTableMetadata != null ? joinTableMetadata.getJoinTableName() : null;
                    String joinColumnName = joinTableMetadata != null
                            ? (String) joinTableMetadata.getJoinColumns().toArray()[0] : null;
                    String inverseJoinColumnName = joinTableMetadata != null
                            ? (String) joinTableMetadata.getInverseJoinColumns().toArray()[0] : null;
                    if (joinTableName != null)
                    {
                        TableInfo joinTableInfo = new TableInfo(joinTableName, Type.COLUMN_FAMILY.name(), String.class,
                                "key");
                        if (!tableInfos.isEmpty() && !tableInfos.contains(joinTableInfo) || tableInfos.isEmpty())
                        {
                            joinTableInfo.addColumnInfo(getJoinColumn(joinTableInfo, joinColumnName,
                                    entityMetadata.getIdAttribute().getJavaType()));
                            joinTableInfo.addColumnInfo(getJoinColumn(joinTableInfo, inverseJoinColumnName,
                                    targetEntityMetadata.getIdAttribute().getJavaType()));

                            // // Do not delete above lines. Currently join
                            // table
                            // columns are of type string only.
                            // // It needs to be fixed later.
                            // joinTableInfo.addColumnInfo(getJoinColumn(joinTableInfo,
                            // joinColumnName, String.class));
                            // joinTableInfo.addColumnInfo(getJoinColumn(joinTableInfo,
                            // inverseJoinColumnName, String.class));

                            tableInfos.add(joinTableInfo);
                        }
                    }
                }
            }
        }
    }

    /**
     * adds join column name to the table Info of entity.
     * 
     * @param joinColumn
     * @param targetTableInfo
     * @param targetTableInfos
     */
    private void addJoinColumnToInfo(String joinColumn, TableInfo targetTableInfo, List<TableInfo> targetTableInfos,
            EntityMetadata m)
    {
        if (!joinColumn.equals(targetTableInfo.getIdColumnName()))
        {

            if (!targetTableInfos.isEmpty() && targetTableInfos.contains(targetTableInfo))
            {
                int idx = targetTableInfos.indexOf(targetTableInfo);
                targetTableInfo = targetTableInfos.get(idx);
                ColumnInfo columnInfoOfJoinColumn = getJoinColumn(targetTableInfo, joinColumn,
                        m.getIdAttribute().getBindableJavaType());
                if (!targetTableInfo.getColumnMetadatas().contains(columnInfoOfJoinColumn))
                {
                    targetTableInfo.addColumnInfo(columnInfoOfJoinColumn);
                }
            }
            else
            {
                ColumnInfo columnInfoOfJoinColumn = getJoinColumn(targetTableInfo, joinColumn,
                        m.getIdAttribute().getBindableJavaType());
                if (!targetTableInfo.getColumnMetadatas().contains(columnInfoOfJoinColumn))
                {
                    targetTableInfo.addColumnInfo(columnInfoOfJoinColumn);
                }
                targetTableInfos.add(targetTableInfo);
            }
        }
    }

    /**
     * Adds column to table info of entity.
     * 
     * @param entityMetadata
     * @param type
     * @param tableInfo
     */
    private void addColumnToTableInfo(EntityMetadata entityMetadata, Type type, TableInfo tableInfo)
    {
        // Add columns to table info.

        Metamodel metaModel = kunderaMetadata.getApplicationMetadata()
                .getMetamodel(entityMetadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());
        Map<String, PropertyIndex> columns = entityMetadata.getIndexProperties();

        // populate columnsToBeIndexed with composite indexes
        for (Entry<String, PropertyIndex> c : columns.entrySet())
        {
            PropertyIndex indexedColumn = c.getValue();
            if (indexedColumn.getIndexType() != null
                    && indexedColumn.getIndexType().toLowerCase().equals(Constants.COMPOSITE))
            {
                IndexInfo indexInfo = new IndexInfo(c.getKey(), indexedColumn.getMax(), indexedColumn.getMin(),
                        indexedColumn.getIndexType(), indexedColumn.getName());
                tableInfo.addToIndexedColumnList(indexInfo);
            }
        }

        Set attributes = entityType.getAttributes();

        Iterator<Attribute> iter = attributes.iterator();
        while (iter.hasNext())
        {
            Attribute attr = iter.next();

            if (!attr.isAssociation())
            {
                if (((MetamodelImpl) metaModel).isEmbeddable(attr.getJavaType()))
                {

                    EmbeddableType embeddable = metaModel.embeddable(attr.getJavaType());

                    EmbeddedColumnInfo embeddedColumnInfo = getEmbeddedColumn(tableInfo, embeddable, attr.getName(),
                            attr.getJavaType(), ((Field) ((Field) attr.getJavaMember())));

                    if (!tableInfo.getEmbeddedColumnMetadatas().contains(embeddedColumnInfo))
                    {
                        tableInfo.addEmbeddedColumnInfo(embeddedColumnInfo);
                    }
                }
                else if (!attr.isCollection() && !((SingularAttribute) attr).isId())
                {

                    if (((Field) attr.getJavaMember()).getAnnotation(Lob.class) != null)
                    {
                        tableInfo.addLobColumnInfo(((AbstractAttribute) attr).getJPAColumnName());
                    }

                    ColumnInfo columnInfo = getColumn(tableInfo, attr,
                            columns != null ? columns.get(((AbstractAttribute) attr).getJPAColumnName()) : null, null);
                    if (!tableInfo.getColumnMetadatas().contains(columnInfo))
                    {
                        tableInfo.addColumnInfo(columnInfo);
                    }
                }
                else if (attr.isCollection()
                        && MetadataUtils.isBasicElementCollectionField((Field) attr.getJavaMember()))
                {
                    CollectionColumnInfo cci = new CollectionColumnInfo();
                    cci.setCollectionColumnName(((AbstractAttribute) attr).getJPAColumnName());
                    cci.setType(attr.getJavaType());
                    cci.setGenericClasses(PropertyAccessorHelper.getGenericClasses((Field) attr.getJavaMember()));

                    tableInfo.addCollectionColumnMetadata(cci);
                    // add if collection column is indexed
                    String jpaName = ((AbstractAttribute) attr).getJPAColumnName();
                    PropertyIndex indexedColumn = columns.get(jpaName);
                    if (indexedColumn != null && indexedColumn.getName() != null)
                    {
                        IndexInfo indexInfo = new IndexInfo(jpaName, indexedColumn.getMax(), indexedColumn.getMin(),
                                indexedColumn.getIndexType(), indexedColumn.getName());
                        tableInfo.addToIndexedColumnList(indexInfo);
                        // Add more if required
                    }
                }
                else if (attr.isCollection()
                        && !MetadataUtils.isBasicElementCollectionField((Field) attr.getJavaMember()))
                {
                    CollectionColumnInfo eci = new CollectionColumnInfo();
                    eci.setCollectionColumnName(((AbstractAttribute) attr).getJPAColumnName());
                    eci.setType(attr.getJavaType());
                    eci.setGenericClasses(PropertyAccessorHelper.getGenericClasses((Field) attr.getJavaMember()));

                    tableInfo.addElementCollectionMetadata(eci);
                    // add if collection column is indexed
                    String jpaName = ((AbstractAttribute) attr).getJPAColumnName();
                    PropertyIndex indexedColumn = columns.get(jpaName);
                    if (indexedColumn != null && indexedColumn.getName() != null)
                    {
                        IndexInfo indexInfo = new IndexInfo(jpaName, indexedColumn.getMax(), indexedColumn.getMin(),
                                indexedColumn.getIndexType(), indexedColumn.getName());
                        tableInfo.addToIndexedColumnList(indexInfo);
                        // Add more if required
                    }
                }

            }
        }

        onInheritedProperty(tableInfo, entityType);
    }

    /**
     * Add {@link DiscriminatorColumn} for schema generation.
     * 
     * @param tableInfo
     *            table info.
     * @param entityType
     *            entity type.
     */
    private void onInheritedProperty(TableInfo tableInfo, EntityType entityType)
    {
        String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();

        if (discrColumn != null)
        {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnName(discrColumn);
            columnInfo.setType(String.class);
            columnInfo.setIndexable(true);

            IndexInfo idxInfo = new IndexInfo(discrColumn);
            tableInfo.addColumnInfo(columnInfo);
            tableInfo.addToIndexedColumnList(idxInfo);
        }
    }

    /**
     * Returns list of configured table/column families.
     * 
     * @param persistenceUnit
     *            persistence unit, for which schema needs to be fetched.
     * 
     * @return list of {@link TableInfo}
     */
    private List<TableInfo> getSchemaInfo(String persistenceUnit)
    {
        List<TableInfo> tableInfos = puToSchemaMetadata.get(persistenceUnit);
        // if no TableInfos for given persistence unit.
        if (tableInfos == null)
        {
            tableInfos = new ArrayList<TableInfo>();
        }
        return tableInfos;
    }

    /**
     * Returns map of entity metdata {@link EntityMetadata}.
     * 
     * @param appMetadata
     *            application metadata
     * @param persistenceUnit
     *            persistence unit
     * @return map of entity metadata.
     */
    private Map<String, EntityMetadata> getEntityMetadataCol(ApplicationMetadata appMetadata, String persistenceUnit)
    {
        Metamodel metaModel = appMetadata.getMetamodel(persistenceUnit);
        Map<String, EntityMetadata> entityMetadataMap = ((MetamodelImpl) metaModel).getEntityMetadataMap();
        return entityMetadataMap;
    }

    /**
     * Get Embedded column info.
     * 
     * @param embeddableType
     * @param embeddableColName
     * @param embeddedEntityClass
     * @param field
     * @return
     */
    private EmbeddedColumnInfo getEmbeddedColumn(TableInfo tableInfo, EmbeddableType embeddableType,
            String embeddableColName, Class embeddedEntityClass, Field field)
    {

        String[] orderByColumns = null;
        if (field.isAnnotationPresent(OrderBy.class))
        {
            OrderBy order = (OrderBy) field.getAnnotation(OrderBy.class);
            orderByColumns = order.value().split("\\s*,\\s*");
        }
        EmbeddedColumnInfo embeddedColumnInfo = new EmbeddedColumnInfo(embeddableType);
        embeddedColumnInfo.setEmbeddedColumnName(embeddableColName);
        Map<String, PropertyIndex> indexedColumns = IndexProcessor.getIndexesOnEmbeddable(embeddedEntityClass);
        List<ColumnInfo> columns = new ArrayList<ColumnInfo>();

        Set attributes = embeddableType.getAttributes();
        Iterator<Attribute> iter = attributes.iterator();

        while (iter.hasNext())
        {
            Attribute attr = iter.next();
            columns.add(getColumn(tableInfo, attr, indexedColumns.get(attr.getName()), orderByColumns));
        }
        embeddedColumnInfo.setColumns(columns);
        return embeddedColumnInfo;
    }

    /**
     * getColumn method return ColumnInfo for the given column
     * 
     * @param Object
     *            of Column.
     * @return Object of ColumnInfo.
     */
    private ColumnInfo getColumn(TableInfo tableInfo, Attribute column, PropertyIndex indexedColumn,
            String[] orderByColumns)
    {
        ColumnInfo columnInfo = new ColumnInfo();

        if (column.getJavaType().isAnnotationPresent(OrderBy.class))
        {
            OrderBy order = (OrderBy) column.getJavaType().getAnnotation(OrderBy.class);
            orderByColumns = order.value().split("\\s*,\\s*");

        }
        columnInfo.setOrderBy(getOrderByColumn(orderByColumns, column));

        if (column.getJavaType().isEnum())
        {
            columnInfo.setType(String.class);
        }
        else
        {
            columnInfo.setType(column.getJavaType());
        }
        columnInfo.setColumnName(((AbstractAttribute) column).getJPAColumnName());
        if (indexedColumn != null && indexedColumn.getName() != null)
        {
            columnInfo.setIndexable(true);
            IndexInfo indexInfo = new IndexInfo(((AbstractAttribute) column).getJPAColumnName(), indexedColumn.getMax(),
                    indexedColumn.getMin(), indexedColumn.getIndexType(), indexedColumn.getName());
            tableInfo.addToIndexedColumnList(indexInfo);
            // Add more if required
        }

        return columnInfo;
    }

    /**
     * getOrderByColumn method return order by value of the column
     * 
     * @param String
     *            [] orderByColumns
     * 
     * @param Attribute
     *            column
     * 
     * @return orderColumnValue.
     */
    private String getOrderByColumn(String[] orderByColumns, Attribute column)
    {

        if (orderByColumns != null)
        {
            for (String orderColumn : orderByColumns)
            {
                String[] orderValue = orderColumn.split("\\s");
                String orderColumnName = orderValue[0].substring(orderValue[0].lastIndexOf('.') + 1);
                String orderColumnValue = orderValue[1];

                if (orderColumnName.equals(((AbstractAttribute) column).getName())
                        || orderColumnName.equals(((AbstractAttribute) column).getJPAColumnName()))
                {
                    return orderColumnValue;
                }
            }
        }
        return null;

    }

    /**
     * getJoinColumn method return ColumnInfo for the join column
     * 
     * @param columnType
     * 
     * @param String
     *            joinColumnName.
     * @return ColumnInfo object columnInfo.
     */
    private ColumnInfo getJoinColumn(TableInfo tableInfo, String joinColumnName, Class columnType)
    {
        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.setColumnName(joinColumnName);
        columnInfo.setIndexable(true);

        IndexInfo indexInfo = new IndexInfo(joinColumnName);
        tableInfo.addToIndexedColumnList(indexInfo);

        columnInfo.setType(columnType);
        return columnInfo;
    }

    /**
     * getKunderaProperty method return auto schema generation property for give
     * persistence unit.
     * 
     * @param externalProperties
     * 
     * @param String
     *            persistenceUnit.
     * @return value of kundera auto ddl in form of String.
     */
    private String getSchemaProperty(String persistenceUnit, Map<String, Object> externalProperties)
    {
        PersistenceUnitMetadata persistenceUnitMetadata = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        String autoDdlOption = externalProperties != null
                ? (String) externalProperties.get(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE) : null;
        if (autoDdlOption == null)
        {
            autoDdlOption = persistenceUnitMetadata != null
                    ? persistenceUnitMetadata.getProperty(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE) : null;
        }
        return autoDdlOption;
    }
}