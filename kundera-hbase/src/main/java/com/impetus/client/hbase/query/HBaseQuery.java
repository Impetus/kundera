package com.impetus.client.hbase.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.client.hbase.HBaseEntityReader;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;

/**
 * @author vivek.mishra
 *
 */
public class HBaseQuery extends QueryImpl implements Query
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseQuery.class);
    
    private EntityReader reader = new HBaseEntityReader();

    /**
     * @param query
     * @param persistenceDelegator
     */
    public HBaseQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
        this.kunderaQuery = kunderaQuery;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        List results = onQuery(m, client);
        ((HBaseClient) client).setFilter(null);
        return results;
    }


    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#recursivelyPopulateEntities(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        // required in case of associated entities.
        List ls = onQuery(m, client);
        ((HBaseClient) client).setFilter(null);
        return setRelationEntities(ls, client, m);
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        return reader;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
    @Override
    protected int onExecuteUpdate()
    {
        if (kunderaQuery.isDeleteUpdate())
        {
            List result = getResultList();
            return result != null ? result.size() : 0;
        }

        return 0;
    }

    

    private List onQuery(EntityMetadata m, Client client)
    {
        // Called only in case of standalone entity.
        QueryTranslator translator = new QueryTranslator();
        translator.translate(getKunderaQuery(), m);
        Map<Boolean, Filter> filter = translator.getFilter();
        if (MetadataUtils.useSecondryIndex(m.getPersistenceUnit()))
        {
            if (filter == null)
            {
                // means complete scan without where clause, scan all records.
                // findAll.
                if (translator.isRangeScan())
                {
                    return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, translator.getStartRow(),
                            translator.getEndRow());
                } else
                {
                    return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, null, null);
                }
            }
            else
            {
                // means WHERE clause is present.
                
                if (filter.values() != null && !filter.values().isEmpty())
                {
                    ((HBaseClient) client).setFilter(filter.values().iterator().next());
                }

                    // if range query. means query over id column. create range
                    // scan method.

                    // else setFilter to client and invoke new method. find by
                    // query if isFindById is false! else invoke findById

                    if (translator.isFindById)
                    {
                        List results = new ArrayList();

                        Object output = client.find(m.getEntityClazz(), translator.rowKey);
                        if (output != null)
                        {
                            results.add(output);
                            return results;
                        }

                    }
                    else
                    {
                        return ((HBaseClient) client).findByQuery(m.getEntityClazz(), m);
                    }
                }
        }
        else
        {
            List results = null;
            return populateUsingLucene(m, client, results);
        }
        return null;
    }

    class QueryTranslator
    {
        private List<Filter> filterList;
        
        private boolean isIdColumn;
        
        private byte[] startRow;
        private byte[] endRow;
        private boolean isFindById;
        String rowKey;
        
        
        void translate(KunderaQuery query, EntityMetadata m)
        {
            String idColumn = m.getIdColumn().getName();
            boolean isIdColumn = false;
            for(Object obj : query.getFilterClauseQueue())
            {
                if (obj instanceof FilterClause)
                {
                    String condition = ((FilterClause) obj).getCondition();
                    String name =  ((FilterClause) obj).getProperty();
                    String value =  ((FilterClause) obj).getValue();
                    if (!isIdColumn && idColumn.equalsIgnoreCase(name))
                    {
                        isIdColumn = true;
                    }
           
                    onParseFilter(condition,name,value,isIdColumn,m);
                    
                } else
                {
                    // Case of AND and OR clause.
                    String opr = obj.toString();
                    if (opr.equalsIgnoreCase("or"))
                    {
                        log.error("Support for OR clause is not enabled with in Hbase");
                        throw new QueryHandlerException("unsupported clause " + opr + " for Hbase");
                    }   
                }
                
            }
        }
        
        Map<Boolean, Filter> getFilter()
        {
            if(filterList != null)
            {
                Map<Boolean, Filter> queryClause = new HashMap<Boolean, Filter>();
                queryClause.put(isIdColumn, new FilterList(filterList));
                return queryClause;
            }
            
            return null;
        }
    
        private void onParseFilter(String condition, String name, String value, boolean isIdColumn, EntityMetadata m)
        {
            CompareOp operator = getOperator(condition, isIdColumn);
            byte[] valueInBytes = getBytes(name, m, value);
            if(!isIdColumn)
            {
                Filter f = new SingleColumnValueFilter(name.getBytes(), name.getBytes(), operator, valueInBytes);
                addToFilter(f);
            } else
            {
                if (operator.equals(CompareOp.GREATER_OR_EQUAL))
                {
                    startRow = valueInBytes;
                }
                else if (operator.equals(CompareOp.LESS_OR_EQUAL))
                {
                    endRow = valueInBytes;
                } else if(operator.equals(CompareOp.EQUAL))
                {
                    rowKey = value;
                    endRow = null;
                    isFindById = true;
                }
            }
            this.isIdColumn = isIdColumn;
        }
    
        
        /**
         * @return the startRow
         */
        byte[] getStartRow()
        {
            return startRow;
        }

       
        /**
         * @return the endRow
         */
        byte[] getEndRow()
        {
            return endRow;
        }


        /**
         * @return the isFindById
         */
        boolean isFindById()
        {
            return isFindById;
        }

        boolean isRangeScan()
        {
            return startRow != null && endRow != null && !isFindById;
        }
        /**
         * @param f
         */
        private void addToFilter(Filter f)
        {
            if(filterList == null)
            {
                filterList = new ArrayList<Filter>();
               
            }
            filterList.add(f);
            
        }

        /**
         * Gets the operator.
         * 
         * @param condition
         *            the condition
         * @param idPresent
         *            the id present
         * @return the operator
         */
        private CompareOp getOperator(String condition, boolean idPresent)
        {
            if (!idPresent && condition.equals("="))
            {
                return CompareOp.EQUAL;
            }
            else if (!idPresent && condition.equals(">"))
            {
                return CompareOp.GREATER;
            }
            else if (!idPresent && condition.equals("<"))
            {
                return CompareOp.LESS;
            }
            else if (condition.equals(">="))
            {
                return CompareOp.GREATER_OR_EQUAL;
            }
            else if (condition.equals("<="))
            {
                return CompareOp.LESS_OR_EQUAL;
            }
            else
            {
                if (!idPresent)
                {
                    throw new UnsupportedOperationException(" Condition " + condition + " is not suported in  cassandra!");
                }
                else
                {
                    throw new UnsupportedOperationException(" Condition " + condition
                            + " is not suported for query on row key!");

                }
            }

        }
    }


    /**
     * Returns bytes value for given value.
     * 
     * @param fieldName
     *            field name.
     * @param m
     *            entity metadata
     * @param value
     *            value.
     * @return bytes value.
     */
    private byte[] getBytes(String fieldName, EntityMetadata m, String value)
    {
        Column idCol = m.getIdColumn();
        Field f = null;
        boolean isId = false;
        if (idCol.getName().equals(fieldName))
        {
            f = idCol.getField();
            isId = true;
        }
        else
        {
            Column col = m.getColumn(fieldName);
            if (col == null)
            {
                throw new QueryHandlerException("column type is null for: " + fieldName);
            }
            f = col.getField();
        }

        if (f != null && f.getType() != null)
        {
            if (isId || f.getType().isAssignableFrom(String.class))
            {

                return Bytes.toBytes(value.trim());
            }
            else if (f.getType().equals(int.class) || f.getType().isAssignableFrom(Integer.class))
            {
                return Bytes.toBytes(Integer.parseInt(value));
            }
            else if (f.getType().equals(long.class) || f.getType().isAssignableFrom(Long.class))
            {

                return Bytes.toBytes(Long.parseLong(value));
            }
            else if (f.getType().equals(boolean.class) || f.getType().isAssignableFrom(Boolean.class))
            {
                return Bytes.toBytes(Boolean.valueOf(value));
            }
            else if (f.getType().equals(double.class) || f.getType().isAssignableFrom(Double.class))
            {
                return Bytes.toBytes(Double.valueOf(value));
            }
            else if (f.getType().isAssignableFrom(java.util.UUID.class))
            {
                return Bytes.toBytes(value);
            }
            else if (f.getType().equals(float.class) || f.getType().isAssignableFrom(Float.class))
            {
                return Bytes.toBytes(Float.valueOf(value));
            }
            else
            {
                log.error("Error while handling data type for:" + fieldName);
                throw new QueryHandlerException("unsupported data type:" + f.getType());
            }
        }
        else
        {
            log.error("Error while handling data type for:" + fieldName);
            throw new QueryHandlerException("field type is null for:" + fieldName);
        }
    }


}
