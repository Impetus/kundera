/*******************************************************************************
 *  * Copyright 2017 Impetus Infotech.
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
package com.impetus.client.couchbase.query;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.x;

import java.util.Iterator;
import java.util.List;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.eclipse.persistence.jpa.jpql.parser.ComparisonExpression;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.JPQLExpression;
import org.eclipse.persistence.jpa.jpql.parser.WhereClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.path.AsPath;
import com.impetus.client.couchbase.CouchbaseClient;
import com.impetus.client.couchbase.CouchbaseConstants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQueryUtils;
import com.impetus.kundera.query.Query;
import com.impetus.kundera.query.QueryImpl;

/**
 * The Class CouchbaseQuery.
 * 
 * @author devender.yadav
 */
public class CouchbaseQuery extends QueryImpl implements Query
{

    /** The LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseQuery.class);

    /**
     * Instantiates a new couchbase query.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public CouchbaseQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            KunderaMetadata kunderaMetadata)
    {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera.
     * metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List populateEntities(EntityMetadata m, Client client)
    {
        if (kunderaQuery.isNative())
        {
            return ((CouchbaseClient) client).executeNativeQuery(getJPAQuery(), m);
        }
        else
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                    .getMetamodel(m.getPersistenceUnit());
            EntityType entityType = metaModel.entity(m.getEntityClazz());
            JPQLExpression jpqlExp = kunderaQuery.getJpqlExpression();
            List<String> selectColumns = KunderaQueryUtils.getSelectColumns(jpqlExp);
            AsPath asPath;
            if (selectColumns.isEmpty())
            {
                asPath = select(CouchbaseConstants.STAR).from(i(m.getSchema()));
            }
            else
            {
                asPath = select(selectColumns.toArray(new String[selectColumns.size()])).from(i(m.getSchema()));
            }
            Statement statement;

            if (KunderaQueryUtils.hasWhereClause(jpqlExp))
            {
                WhereClause whereClause = KunderaQueryUtils.getWhereClause(jpqlExp);
                Expression whereExp = whereClause.getConditionalExpression();

                if (whereExp instanceof ComparisonExpression)
                {
                    String left = ((ComparisonExpression) whereExp).getLeftExpression().toActualText();
                    String right = ((ComparisonExpression) whereExp).getRightExpression().toActualText();
                    if (right.startsWith(CouchbaseConstants.POSITIONAL_PREFIX)
                            || right.startsWith(CouchbaseConstants.PARAMETERIZED_PREFIX))
                    {
                        right = kunderaQuery.getParametersMap().get(right) + "";
                    }
                    Attribute attribute = entityType.getAttribute(left.split(CouchbaseConstants.DOT_REGEX)[1]);

                    statement = addWhereCondition(asPath, ((ComparisonExpression) whereExp).getActualIdentifier(),
                            ((AbstractAttribute) attribute).getJPAColumnName(), right, m.getTableName());
                }

            }
            else
            {
                asPath.where(x(CouchbaseConstants.KUNDERA_ENTITY).eq(x("'" + m.getTableName() + "'")));
            }

            statement = asPath;
            return ((CouchbaseClient) client).executeQuery(statement, m);
        }
    }

    /**
     * Adds the where condition.
     *
     * @param asPath
     *            the as path
     * @param identifier
     *            the identifier
     * @param colName
     *            the col name
     * @param val
     *            the val
     * @param tableName
     *            the table name
     * @return the statement
     */
    public Statement addWhereCondition(AsPath asPath, String identifier, String colName, String val, String tableName)
    {
        com.couchbase.client.java.query.dsl.Expression exp;
        switch (identifier)
        {

        case "<":
            exp = x(colName).lt(x(val));
            break;
        case "<=":
            exp = x(colName).lte(x(val));
            break;
        case ">":
            exp = x(colName).gt(x(val));
            break;
        case ">=":
            exp = x(colName).gte(x(val));
            break;
        case "=":
            exp = x(colName).eq(x(val));
            break;
        default:
            LOGGER.error("Operator " + identifier + "  is not supported in the JPA query for Couchbase.");
            throw new KunderaException("Operator " + identifier + "  is not supported in the JPA query for Couchbase.");
        }

        return asPath.where(exp.and(x(CouchbaseConstants.KUNDERA_ENTITY).eq(x("'" + tableName) + "'")));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#findUsingLucene(com.impetus.kundera.
     * metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List findUsingLucene(EntityMetadata m, Client client)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#recursivelyPopulateEntities(com.
     * impetus.kundera.metadata.model.EntityMetadata ,
     * com.impetus.kundera.client.Client)
     */
    @Override
    protected List recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
    @Override
    protected int onExecuteUpdate()
    {
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#close()
     */
    @Override
    public void close()
    {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#iterate()
     */
    @Override
    public Iterator iterate()
    {
        return null;
    }

}
