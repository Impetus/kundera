/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.query;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.impetus.kundera.index.IndexingConstants;

/**
 * Builder interface to build lucene query.
 * 
 * @author vivek.mishra
 * 
 */
public final class LuceneQueryBuilder
{

    private static final Map<String, condition> conditions = new HashMap<String, LuceneQueryBuilder.condition>();

    private StringBuilder builder = new StringBuilder();

    public static enum condition
    {
        EQ, LIKE, GT, LT, LTE, GTE, AND, OR, NOT_EQ, IN;
    }

    private static final String LUCENE_ESCAPE_CHARS = "[\\\\+\\-\\!\\(\\)\\:\\^\\]\\{\\}\\~\\*\\?]";

    private static final Pattern LUCENE_PATTERN = Pattern.compile(LUCENE_ESCAPE_CHARS);

    private static final String REPLACEMENT_STRING = "\\\\$0";

    static
    {
        conditions.put("=", condition.EQ);
        conditions.put("like", condition.LIKE);
        conditions.put(">", condition.GT);
        conditions.put("<", condition.LT);
        conditions.put(">=", condition.GTE);
        conditions.put("<=", condition.LTE);
        conditions.put("and", condition.AND);
        conditions.put("or", condition.OR);
        conditions.put("<>", condition.NOT_EQ);
        conditions.put("in", condition.IN);
    }

    /**
     * @param condition
     * @param builder
     *            Code inspired :
     *            http://www.javalobby.org/java/forums/t86124.html
     */
    public final LuceneQueryBuilder buildQuery(final String condition, final String value, final Class valueClazz)
    {

        condition c = conditions.get(condition.toLowerCase().trim());
        String lucenevalue = LUCENE_PATTERN.matcher(value).replaceAll(REPLACEMENT_STRING);
        if (c != null)
            switch (c)
            {
            case EQ:
                builder.append(":");
                builder.append("\"");
                builder.append(lucenevalue);
                builder.append("\"");
                break;

            case NOT_EQ:
                builder.append(":(* NOT ");
                builder.append("\"");
                builder.append(lucenevalue);
                builder.append("\")");
                break;

            case LIKE:
                builder.append(":");
                builder.append("(");
                matchMode(lucenevalue.trim());
                builder.append(")");
                break;

            case GT:
                builder.append(appendRange(lucenevalue, false, true, valueClazz));
                break;

            case LT:
                builder.append(appendRange(lucenevalue, false, false, valueClazz));
                break;

            case GTE:
                builder.append(appendRange(lucenevalue, true, true, valueClazz));
                break;

            case LTE:
                builder.append(appendRange(lucenevalue, true, false, valueClazz));
                break;

            case IN:
                builder.append(":");
                builder.append("(");
                builder.append(value);
                builder.append(")");
                break;

            default:
                builder.append(" " + lucenevalue + " ");
                break;
            }

        return this;
    }

    public LuceneQueryBuilder appendEntityName(final String entityName)
    {

        // add Entity_CLASS field too.
        if (builder.length() > 0)
        {
            builder.insert(0, "(");
            builder.append(")");
            builder.append(" AND ");
        }
        // sb.append("+");
        builder.append(IndexingConstants.ENTITY_CLASS_FIELD);
        builder.append(":");
        // sb.append(getEntityClass().getName());
        builder.append(entityName);
        return this;
    }

    public LuceneQueryBuilder appendIndexName(final String indexName)
    {
        builder.append(indexName);
        builder.append(".");
        return this;
    }

    public LuceneQueryBuilder appendPropertyName(final String propertyName)
    {
        builder.append(propertyName);
        return this;
    }

    public final String getQuery()
    {
        return builder.toString();
    }

    /**
     * Append range.
     * 
     * @param value
     *            the value
     * @param inclusive
     *            the inclusive
     * @param isGreaterThan
     *            the is greater than
     * @return the string
     */
    private String appendRange(final String value, final boolean inclusive, final boolean isGreaterThan,
            final Class clazz)
    {
        String appender = " ";
        StringBuilder sb = new StringBuilder();
        sb.append(":");
        sb.append(inclusive ? "[" : "{");
        sb.append(isGreaterThan ? value : "*");
        sb.append(appender);
        sb.append("TO");
        sb.append(appender);
        // composite key over lucene is not working issue #491
        if (clazz != null
                && (clazz.isAssignableFrom(int.class) || clazz.isAssignableFrom(Integer.class)
                        || clazz.isAssignableFrom(short.class) || clazz.isAssignableFrom(long.class)
                        || clazz.isAssignableFrom(Timestamp.class) || clazz.isAssignableFrom(Long.class)
                        || clazz.isAssignableFrom(float.class) || clazz.isAssignableFrom(Float.class)
                        || clazz.isAssignableFrom(BigDecimal.class) || clazz.isAssignableFrom(Double.class) || clazz
                            .isAssignableFrom(double.class)))
        {
            sb.append(isGreaterThan ? "*" : value);

        }
        else
        {

            sb.append(isGreaterThan ? "null" : value);
        }

        sb.append(inclusive ? "]" : "}");
        return sb.toString();
    }

    /**
     * @param value
     *            checks if value contains % and replaces with * default: if no
     *            % is found.. replaces both sides
     */
    private void matchMode(String value)
    {
        boolean left = false;
        boolean right = false;

        if (value.charAt(0) == '%')
        {
            value = value.substring(1);
            left = true;
        }
        if (value.charAt(value.length() - 1) == '%')
        {
            value = value.substring(0, value.length() - 1);
            right = true;
        }
        if ((left && right) || (!left && !right))
        {
            builder.append("*");
            builder.append(value);
            builder.append("*");
        }
        else if (left)
        {
            builder.append("*");
            builder.append(value);
        }
        else if (right)
        {
            builder.append(value);
            builder.append("*");
        }

    }

}