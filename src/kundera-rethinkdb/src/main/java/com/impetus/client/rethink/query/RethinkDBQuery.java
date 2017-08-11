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
package com.impetus.client.rethink.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.persistence.Id;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.apache.commons.beanutils.ConvertUtils;
import org.eclipse.persistence.jpa.jpql.parser.ComparisonExpression;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.JPQLExpression;
import org.eclipse.persistence.jpa.jpql.parser.WhereClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.rethink.RethinkDBClient;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQueryUtils;
import com.impetus.kundera.query.Query;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Filter;
import com.rethinkdb.gen.ast.Pluck;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

/**
 * The Class RethinkDBQuery.
 * 
 * @author karthikp.manchala
 */
public class RethinkDBQuery extends QueryImpl implements Query
{

    /** The Constant ID. */
    private static final String ID = "id";

    /** The Constant DOT_REGEX. */
    private static final String DOT_REGEX = "[.]";

    /** The Constant PARAMETERIZED_PREFIX. */
    private static final String PARAMETERIZED_PREFIX = ":";

    /** The Constant POSITIONAL_PREFIX. */
    private static final String POSITIONAL_PREFIX = "?";

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RethinkDBQuery.class);

    /**
     * Instantiates a new rethink db query.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public RethinkDBQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            KunderaMetadata kunderaMetadata)
    {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
        // TODO Auto-generated constructor stub
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
        RethinkDB r = ((RethinkDBClient) client).getR();
        Connection conn = ((RethinkDBClient) client).getConnection();

        Filter filter = null;
        Pluck pluck;
        Table table = r.db(m.getSchema()).table(m.getTableName());

        Cursor cursor = null;
        List results = new ArrayList();

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());

        JPQLExpression jpqlExp = kunderaQuery.getJpqlExpression();
        List<String> selectColumns = KunderaQueryUtils.getSelectColumns(jpqlExp);

        if (KunderaQueryUtils.hasWhereClause(jpqlExp))
        {
            // add filters
            WhereClause whereClause = KunderaQueryUtils.getWhereClause(jpqlExp);
            Expression whereExp = whereClause.getConditionalExpression();
            filter = table.filter(parseAndBuildFilters(entityType, whereExp));
        }

        if (!selectColumns.isEmpty())
        {
            // select by specific columns, consider last for plucking
            pluck = filter == null ? table.pluck(selectColumns) : filter.pluck(selectColumns);
            cursor = pluck.run(conn);
        }

        if (cursor == null)
        {
            cursor = (Cursor) (filter == null ? table.run(conn) : filter.run(conn));
        }

        for (Object obj : cursor)
        {
            Object entity = KunderaCoreUtils.createNewInstance(m.getEntityClazz());
            buildEntityFromCursor(entity, (HashMap) obj, entityType);
            results.add(entity);
        }

        return results;
    }

    /**
     * Builds the entity from cursor.
     * 
     * @param entity
     *            the entity
     * @param obj
     *            the obj
     * @param entityType
     *            the entity type
     * @return the object
     */
    private void buildEntityFromCursor(Object entity, HashMap obj, EntityType entityType)
    {
        Iterator<Attribute> iter = entityType.getAttributes().iterator();
        while (iter.hasNext())
        {
            Attribute attribute = iter.next();
            Field field = (Field) attribute.getJavaMember();
            if (field.isAnnotationPresent(Id.class))
            {
                PropertyAccessorHelper.set(entity, field, obj.get(ID));
            }
            else
            {
                PropertyAccessorHelper.set(entity, field, obj.get(((AbstractAttribute) attribute).getJPAColumnName()));
            }

        }
    }

    /**
     * Parses the and build filters.
     * 
     * @param entityType
     *            the entity type
     * @param whereExp
     *            the where exp
     * @return the reql function1
     */
    private ReqlFunction1 parseAndBuildFilters(EntityType entityType, Expression whereExp)
    {
        if (whereExp instanceof ComparisonExpression)
        {
            String left = ((ComparisonExpression) whereExp).getLeftExpression().toActualText();

            String right = ((ComparisonExpression) whereExp).getRightExpression().toActualText();
            right = right.replaceAll("['\"]", "");
            if (right.startsWith(POSITIONAL_PREFIX) || right.startsWith(PARAMETERIZED_PREFIX))
            {
                right = kunderaQuery.getParametersMap().get(right) + "";
            }
            Attribute attribute = entityType.getAttribute(left.split(DOT_REGEX)[1]);
            boolean isId = ((Field) attribute.getJavaMember()).isAnnotationPresent(Id.class);
          
            return buildFunction(isId ? ID : ((AbstractAttribute) attribute).getJPAColumnName(),
                    ConvertUtils.convert(right, attribute.getJavaType()),
                    ((ComparisonExpression) whereExp).getActualIdentifier());
        }
        else
        {
            logger.error("Operation not supported");
            throw new KunderaException("Operation not supported");
        }
    }

    /**
     * Builds the function.
     * 
     * @param colName
     *            the col name
     * @param obj
     *            the obj
     * @param identifier
     *            the identifier
     * @return the reql function1
     */
    public static ReqlFunction1 buildFunction(final String colName, final Object obj, final String identifier)
    {
        return new ReqlFunction1()
        {

            @Override
            public Object apply(ReqlExpr row)
            {

                switch (identifier)
                {

                case "<":
                    return row.g(colName).lt(obj);

                case "<=":
                    return row.g(colName).le(obj);

                case ">":
                    return row.g(colName).gt(obj);
                case ">=":
                    return row.g(colName).ge(obj);

                case "=":
                    return row.g(colName).eq(obj);

                default:
                    logger.error("Operation not supported");
                    throw new KunderaException("Operation not supported");

                }
            }
        };
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
        // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#iterate()
     */
    @Override
    public Iterator iterate()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
