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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.metamodel.Metamodel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.EmbeddedColumnInfo;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.TableInfo.SchemaAction;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;

public class SchemaConfiguration implements Configuration
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(SchemaConfiguration.class);

    /** Holding persistence unit instances. */
    private String[] persistenceUnits;

    private Map<String, List<TableInfo>> puToSchemaCol;

    /**
     * Constructor using persistence units as parameter.
     * 
     * @param persistenceUnits
     *            persistence units.
     */
    public SchemaConfiguration(String... persistenceUnits)
    {
        this.persistenceUnits = persistenceUnits;
    }

    @Override
    public void configure()
    {

        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        puToSchemaCol = appMetadata.getSchemaMetadata().getPuToSchemaCol();

        //TODO, FIXME: Refactoring is required.
        for (String persistenceUnit : persistenceUnits)
        {
            if (getKunderaProperty(persistenceUnit) != null)
            {
                List<TableInfo> tableInfos = puToSchemaCol.get(persistenceUnit);
                // if no TableInfos for given persistence unit.
                if (tableInfos == null)
                {
                    tableInfos = new ArrayList<TableInfo>();
                }

                Metamodel metaModel = appMetadata.getMetamodel(persistenceUnit);
                Map<Class<?>, EntityMetadata> entityMetadataMap = ((MetamodelImpl) metaModel).getEntityMetadataMap();

                for (EntityMetadata entityMetadata : entityMetadataMap.values())
                {
                    // get entity metadata(table info as well as columns)
                    // if table info exists, get it from map.
                    boolean found = false;
                    Type type = entityMetadata.getType();
                    TableInfo tableInfo = new TableInfo();
                    tableInfo.setTableName(entityMetadata.getTableName());
                    tableInfo.setIndexable(entityMetadata.isIndexable());

                    // check for standard and super column.
                    if (type.isColumnFamilyMetadata())
                    {
                        tableInfo.setType("Standard");
                    }
                    else
                    {
                        tableInfo.setType("Super");
                    }
                    tableInfo.setTableIdType(entityMetadata.getIdColumn().getField().getType().toString());
                    tableInfo.setAction(SchemaAction.instanceOf(getKunderaProperty(persistenceUnit)));

                    // check for tableInfos not empty and contains the present
                    // tableInfo.
                    if (!tableInfos.isEmpty() && tableInfos.contains(tableInfo))
                    {
                        found = true;
                        int idx = tableInfos.indexOf(tableInfo);
                        tableInfo = tableInfos.get(idx);
                        if (tableInfo.getType().equalsIgnoreCase("Standard"))
                        {
                            for (Column column : entityMetadata.getColumnsAsList())
                            {
                                if (!tableInfo.getColumnMetadatas().contains(getColumn(column)))
                                {
                                    tableInfo.getColumnMetadatas().add(getColumn(column));
                                }
                            }
                        }
                        else
                        {
                            List<EmbeddedColumnInfo> embeddedColumnInfos = new ArrayList<EmbeddedColumnInfo>();
                            for (EmbeddedColumn embeddedColumn : entityMetadata.getEmbeddedColumnsAsList())
                            {
                                embeddedColumnInfos.add(getEmbeddedColumn(embeddedColumn));
                                tableInfo.setEmbeddedColumnMetadatas(embeddedColumnInfos);
                            }
                        }
                    }
                    else
                    {
                        if (tableInfo.getType().equalsIgnoreCase("Standard"))
                        {
                            List<ColumnInfo> columns = new ArrayList<ColumnInfo>();

                            for (Column column : entityMetadata.getColumnsAsList())
                            {
                                columns.add(getColumn(column));
                            }
                            tableInfo.setColumnMetadatas(columns);
                        }
                        else
                        {
                            List<EmbeddedColumnInfo> embeddedColumnInfos = new ArrayList<EmbeddedColumnInfo>();
                            for (EmbeddedColumn embeddedColumn : entityMetadata.getEmbeddedColumnsAsList())
                            {
                                embeddedColumnInfos.add(getEmbeddedColumn(embeddedColumn));
                            }
                            tableInfo.setEmbeddedColumnMetadatas(embeddedColumnInfos);
                        }
                    }
                    List<Relation> relations = entityMetadata.getRelations();
                    for (Relation relation : relations)
                    {
                        Class entityClass = relation.getTargetEntity();
                        EntityMetadata targetEntityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
                        ForeignKey relationType = relation.getType();

                        if ((relationType.equals(ForeignKey.ONE_TO_MANY) && relation.getJoinColumnName() != null)
                                || relation.isJoinedByPrimaryKey())
                        {
                            if (targetEntityMetadata.equals(entityMetadata))
                            {
                                if (tableInfo.getColumnMetadatas() == null)
                                {
                                    List<ColumnInfo> columnMetadatas = new ArrayList<ColumnInfo>();
                                    columnMetadatas.add(getJoinColumn(relation.getJoinColumnName()));
                                    tableInfo.setColumnMetadatas(columnMetadatas);
                                }
                                else if (tableInfo.getColumnMetadatas().contains(
                                        getJoinColumn(relation.getJoinColumnName())))
                                {
                                    tableInfo.getColumnMetadatas().add(getJoinColumn(relation.getJoinColumnName()));
                                }
                            }
                            else
                            {
                                String pu = targetEntityMetadata.getPersistenceUnit();
                                Type targetEntityType = targetEntityMetadata.getType();
                                TableInfo targetTableInfo = new TableInfo();
                                targetTableInfo.setTableName(targetEntityMetadata.getTableName());
                                targetTableInfo.setIndexable(targetEntityMetadata.isIndexable());
                                if (targetEntityType.isColumnFamilyMetadata())
                                {
                                    targetTableInfo.setType("Standard");
                                }
                                else
                                {
                                    targetTableInfo.setType("Super");
                                }
                                targetTableInfo.setTableIdType(targetEntityMetadata.getIdColumn().getField().getType()
                                        .toString());
                                targetTableInfo.setAction(SchemaAction.instanceOf(getKunderaProperty(persistenceUnit)));
                                if (!pu.equals(persistenceUnit))
                                {
                                    List<TableInfo> targetTableInfos = puToSchemaCol.get(pu);
                                    if (targetTableInfos == null)
                                    {
                                        targetTableInfos = new ArrayList<TableInfo>();
                                    }

                                    if (!targetTableInfos.isEmpty() && targetTableInfos.contains(targetTableInfo))
                                    {
                                        int idx = targetTableInfos.indexOf(targetTableInfo);
                                        targetTableInfo = targetTableInfos.get(idx);

                                        Column column = entityMetadata.getIdColumn();
                                        targetTableInfo.getColumnMetadatas().add(getColumn(column));
                                        targetTableInfos.add(targetTableInfo);
                                    }
                                    else
                                    {
                                        List<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();
                                        columnInfos.add(getColumn(entityMetadata.getIdColumn()));
                                        targetTableInfo.setColumnMetadatas(columnInfos);
                                        targetTableInfos.add(targetTableInfo);
                                    }
                                    puToSchemaCol.put(pu, targetTableInfos);
                                }
                                else
                                {
                                    if (!tableInfos.isEmpty() && tableInfos.contains(targetTableInfo))
                                    {
                                        int idx = tableInfos.indexOf(targetTableInfo);
                                        targetTableInfo = tableInfos.get(idx);

                                        Column column = entityMetadata.getIdColumn();
                                        if (!targetTableInfo.getColumnMetadatas().contains(getColumn(column)))
                                        {
                                            targetTableInfo.getColumnMetadatas().add(getColumn(column));
                                        }
                                    }
                                    else
                                    {
                                        List<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();
                                        columnInfos.add(getColumn(entityMetadata.getIdColumn()));
                                        targetTableInfo.setColumnMetadatas(columnInfos);
                                        tableInfos.add(targetTableInfo);
                                    }
                                }
                            }
                        }
                        else if (relation.isUnary() && relation.getJoinColumnName() != null)
                        {
                            if (tableInfo.getColumnMetadatas() == null)
                            {
                                List<ColumnInfo> columnMetadatas = new ArrayList<ColumnInfo>();
                                columnMetadatas.add(getJoinColumn(relation.getJoinColumnName()));
                                tableInfo.setColumnMetadatas(columnMetadatas);
                            }
                            else if (!tableInfo.getColumnMetadatas().contains(
                                    getJoinColumn(relation.getJoinColumnName())))
                            {
                                tableInfo.getColumnMetadatas().add(getJoinColumn(relation.getJoinColumnName()));
                            }
                        }
                    }
                    if (!found)
                    {
                        tableInfos.add(tableInfo);
                    }
                }
                puToSchemaCol.put(persistenceUnit, tableInfos);
            }
        }
    }

    /**
     * getEmbeddedColumn method return EmbeddedColumnInfo for the given
     * EmbeddedColumn.
     * 
     * @param object
     *            of EmbeddedColumn.
     * @return EmbeddedColumnInfo object.
     */
    private EmbeddedColumnInfo getEmbeddedColumn(EmbeddedColumn embeddedColumn)
    {
        EmbeddedColumnInfo embeddedColumnInfo = new EmbeddedColumnInfo();
        embeddedColumnInfo.setEmbeddedColumnName(embeddedColumn.getName());
        List<ColumnInfo> columns = new ArrayList<ColumnInfo>();
        for (Column column : embeddedColumn.getColumns())
        {
            columns.add(getColumn(column));
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
    private ColumnInfo getColumn(Column column)
    {
        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.setColumnName(column.getName());
        columnInfo.setIndexable(column.isIndexable());
        columnInfo.setType(column.getField().getType().toString());
        return columnInfo;
    }

    /**
     * getJoinColumn method return ColumnInfo for the join column
     * 
     * @param String
     *            joinColumnName.
     * @return ColumnInfo object columnInfo.
     */
    private ColumnInfo getJoinColumn(String joinColumnName)
    {
        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.setColumnName(joinColumnName);
        columnInfo.setIndexable(true);
        columnInfo.setType(joinColumnName);
        return columnInfo;
    }

    /**
     * getKunderaProperty method return auto schema generation property for give
     * persistence unit.
     * 
     * @param String
     *            persistenceUnit.
     * @return value of kundera auto ddl in form of String.
     */
    private String getKunderaProperty(String persistenceUnit)
    {
        String KUNDERA_DDL_AUTO_PREPARE = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit)
                .getProperty(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE);

        return KUNDERA_DDL_AUTO_PREPARE;
    }

  
}