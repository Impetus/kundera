/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.client.cassandra.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Query;

import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.client.cassandra.pelops.PelopsClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.query.exception.QueryHandlerException;

/**
 * The Class CassQuery.
 *
 * @author vivek.mishra
 */
public class CassQuery extends QueryImpl implements Query
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(CassQuery.class);

    private EntityReader reader;

    /**
     * Instantiates a new cass query.
     *
     * @param query
     *            the query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param persistenceUnits
     *            the persistence units
     */
    public CassQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            String[] persistenceUnits)
    {
        super(query, persistenceDelegator, persistenceUnits);
        this.kunderaQuery = kunderaQuery;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        log.debug("on populateEntities cassandra query");
        List<Object> result = null;
        if (MetadataUtils.useSecondryIndex(m.getPersistenceUnit()))
        {

            List<IndexClause> ixClause = prepareIndexClause(m);

            result = ((PelopsClient) client).find(ixClause, m, false, null);
        }
        else
        {
            result = populateUsingLucene(m, client, result);

        }
        return result;
    }

    private List<IndexClause> prepareIndexClause(EntityMetadata m)
    {
        IndexClause indexClause = Selector.newIndexClause(Bytes.EMPTY, 10000);
        List<IndexClause> clauses = new ArrayList<IndexClause>();
        List<IndexExpression> expr = new ArrayList<IndexExpression>();
        for (Object o : getKunderaQuery().getFilterClauseQueue())
        {
            if (o instanceof FilterClause)
            {
                FilterClause clause = ((FilterClause) o);
                String fieldName = getColumnName(clause.getProperty());
                String condition = clause.getCondition();
                String value = clause.getValue();
                expr.add(Selector.newIndexExpression(fieldName, getOperator(condition),getBytesValue(fieldName, m, value)));
            }
            else
            {
                // Case of AND and OR clause.
                String opr = o.toString();
                if (opr.equalsIgnoreCase("or"))
                {
/*                    indexClause.setExpressions(expr);
                    clauses.add(indexClause);
                    indexClause = Selector.newIndexClause(Bytes.EMPTY, Integer.SIZE);
                    expr = new ArrayList<IndexExpression>();
*/
                    log.error("Support for OR clause is not enabled with in cassandra");
                    throw new QueryHandlerException("unsupported clause " + opr + " for cassandra");
                 }

                // TODO need to handle scenario for AND + OR .

            }
        }
        indexClause.setExpressions(expr);
        clauses.add(indexClause);

        return clauses;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.impetus.kundera.query.QueryImpl#handleAssociations(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client,
     * java.util.List, java.util.List, boolean)
     */
    @Override
    protected List<Object> handleAssociations(EntityMetadata m, Client client, List<EntitySaveGraph> graphs,
            List<String> relationNames, boolean isParent)
    {
        log.debug("on handleAssociations rdbms query");
        List<IndexClause> ixClause = prepareIndexClause(m);

        ((CassandraEntityReader) getReader()).setConditions(ixClause);

        List<EnhanceEntity> ls = reader.populateRelation(m, relationNames, isParent, client);

        return handleGraph(ls, graphs, client, m);
    }

    private IndexOperator getOperator(String condition)
    {
        if (condition.equals("="))
        {
            return IndexOperator.EQ;
        }
        else if (condition.equals(">"))
        {
            return IndexOperator.GT;
        }
        else if (condition.equals("<"))
        {
            return IndexOperator.LT;
        }
        else if (condition.equals(">="))
        {
            return IndexOperator.GTE;
        }
        else if (condition.equals("<="))
        {
            return IndexOperator.LTE;
        }
        else
        {
            throw new UnsupportedOperationException(" Condition " + condition + " is not suported in  cassandra!");
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        if (reader == null)
        {
            reader = new CassandraEntityReader(getLuceneQueryFromJPAQuery());
        }

        return reader;
    }


    private Bytes getBytesValue(String fieldName, EntityMetadata m, String value)
    {
        Column col = m.getColumn(fieldName);
        Field f = col.getField();

       if(f.getType() != null)
        {
            if (f.getType().isAssignableFrom(String.class))
            {
                return Bytes.fromByteArray(value.getBytes());
            }
            else if (f.getType().equals(int.class) || f.getType().isAssignableFrom(Integer.class))
            {
                return Bytes.fromInt(Integer.parseInt(value));
            }
            else if (f.getType().equals(long.class) || f.getType().isAssignableFrom(Long.class))
            {
                return Bytes.fromLong(Long.parseLong(value));
            }
            else if (f.getType().equals(boolean.class) || f.getType().isAssignableFrom(Boolean.class))
            {
                return Bytes.fromBoolean(Boolean.valueOf(value));
            }
            else if (f.getType().equals(double.class) || f.getType().isAssignableFrom(Double.class))
            {
                return Bytes.fromDouble(Double.valueOf(value));
            }
            else if (f.getType().isAssignableFrom(java.util.UUID.class))
            {
                return Bytes.fromUuid(value);
            }
            else if (f.getType().equals(float.class) || f.getType().isAssignableFrom(Float.class))
            {
                return Bytes.fromFloat(Float.valueOf(value));
            }
            else
            {
                log.error("Error while handling data type for:" + fieldName);
                throw new QueryHandlerException("unsupported data type:" + f.getType());
            }
        }else
       {
           log.error("Error while handling data type for:" + fieldName);
           throw new QueryHandlerException("field type is null for:" + fieldName);
       }
    }
}
