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
package com.impetus.kundera.rest.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.persistence.EntityManager;
import javax.persistence.Parameter;
import javax.persistence.Query;
import javax.ws.rs.HttpMethod;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryImpl;

/**
 * Utility methods for handling entities passed in REST request
 * 
 * @author amresh
 * 
 */
public class EntityUtils {

    public static Map<String, String> httpMethods = new HashMap<String, String>();

    private static Logger log = LoggerFactory.getLogger(EntityUtils.class);

    static {
        httpMethods.put(HttpMethod.GET, "SELECT");
        httpMethods.put(HttpMethod.POST, "INSERT");
        httpMethods.put(HttpMethod.PUT, "UPDATE");
        httpMethods.put(HttpMethod.DELETE, "DELETE");
    }

    /**
     * @param entityClassName
     * @param em
     * @return
     */
    public static AbstractManagedType getEntityManagedType(String entityClassName, EntityManager em) {
        MetamodelImpl metamodel = (MetamodelImpl) em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);
        EntityMetadata entityMetadata = metamodel.getEntityMetadata(entityClass);
        AbstractManagedType managedType = (AbstractManagedType) metamodel.entity(entityMetadata.getEntityClazz());
        return managedType;
    }

    /**
     * @param entityClassName
     * @param em
     * @return
     */
    public static Class<?> getEntityClass(String entityClassName, EntityManager em) {
        MetamodelImpl metamodel = (MetamodelImpl) em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);
        return entityClass;
    }

    /**
     * @param entityClassName
     * @param em
     * @return
     */
    public static EntityMetadata getEntityMetaData(String entityClassName, EntityManager em) {
        MetamodelImpl metamodel = (MetamodelImpl) em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);

        return metamodel.getEntityMetadata(entityClass);
    }

    public static String getQueryPart(String fullQueryString) {
        if (fullQueryString.contains("?")) {
            return fullQueryString.substring(0, fullQueryString.indexOf("?"));
        } else {
            return fullQueryString;
        }
    }

    public static String getParameterPart(String fullQueryString) {
        if (fullQueryString.contains("?")) {
            return fullQueryString.substring(fullQueryString.indexOf("?") + 1, fullQueryString.length());
        } else {
            return "";
        }
    }

    /**
     * @param queryString
     * @param q
     * @param em
     */
    public static void setObjectQueryParameters(String queryString, String parameterString, Query q, EntityManager em,
        String mediaType) {
        MetamodelImpl metamodel = (MetamodelImpl) em.getEntityManagerFactory().getMetamodel();

        if (parameterString == null || parameterString.isEmpty()) {
            return;
        }

        Map<String, String> paramsMap = new HashMap<String, String>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            paramsMap = mapper.readValue(parameterString, new TypeReference<HashMap<String, String>>() {
            });
            KunderaQuery kq = ((QueryImpl) q).getKunderaQuery();
            Set<Parameter<?>> parameters = kq.getParameters();
            for (String paramName : paramsMap.keySet()) {
                String value = paramsMap.get(paramName);

                if (paramName.equalsIgnoreCase("firstResult")) {
                    q.setFirstResult(Integer.parseInt(value));

                } else if (paramName.equalsIgnoreCase("maxResult")) {
                    q.setMaxResults(Integer.parseInt(value));

                } else if (StringUtils.isNumeric(paramName)) {
                    for (Parameter param : parameters) {
                        if (param.getPosition() == Integer.parseInt(paramName)) {

                            Class<?> paramClass = param.getParameterType();
                            Object paramValue = null;
                            if (metamodel.isEmbeddable(paramClass)) {
                                paramValue =
                                    JAXBUtils.toObject(StreamUtils.toInputStream(value), paramClass, mediaType);

                            } else {
                                PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(paramClass);
                                paramValue = accessor.fromString(paramClass, value);
                            }

                            q.setParameter(Integer.parseInt(paramName), paramValue);
                            break;
                        }
                    }
                } else {
                    for (Parameter param : parameters) {
                        if (param.getName().equals(paramName)) {

                            Class<?> paramClass = param.getParameterType();
                            Object paramValue = null;

                            if (metamodel.isEmbeddable(paramClass)) {
                                paramValue =
                                    JAXBUtils.toObject(StreamUtils.toInputStream(value), paramClass, mediaType);

                            } else {
                                PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(paramClass);
                                paramValue = accessor.fromString(paramClass, value);
                            }
                            q.setParameter(paramName, paramValue);
                            break;
                        }
                    }

                }
            }

        } catch (JsonParseException e) {
            log.error(e.getMessage());
        } catch (JsonMappingException e) {
            log.error(e.getMessage());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * @param queryString
     * @param q
     */
    public static void setQueryParameters(String queryString, String parameterString, Query q) {
        Map<String, String> paramsMap = new HashMap<String, String>();

        StringTokenizer st = new StringTokenizer(parameterString, "&");
        while (st.hasMoreTokens()) {
            String element = st.nextToken();
            paramsMap.put(element.substring(0, element.indexOf("=")),
                element.substring(element.indexOf("=") + 1, element.length()));
        }
        KunderaQuery kq = ((QueryImpl) q).getKunderaQuery();
        Set<Parameter<?>> parameters = kq.getParameters();
        for (String paramName : paramsMap.keySet()) {
            String value = paramsMap.get(paramName);
            if (paramName.equalsIgnoreCase("firstResult")) {
                q.setFirstResult(Integer.parseInt(value));
            } else if (paramName.equalsIgnoreCase("maxResult")) {
                q.setMaxResults(Integer.parseInt(value));
            } else if (StringUtils.isNumeric(paramName)) {
                for (Parameter param : parameters) {
                    if (param.getPosition() == Integer.parseInt(paramName)) {
                        Class<?> paramClass = param.getParameterType();
                        PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(paramClass);
                        Object paramValue = accessor.fromString(paramClass, value);
                        q.setParameter(Integer.parseInt(paramName), paramValue);
                        break;
                    }
                }
            } else {
                for (Parameter param : parameters) {
                    if (param.getName().equals(paramName)) {
                        Class<?> paramClass = param.getParameterType();
                        PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(paramClass);
                        Object paramValue = accessor.fromString(paramClass, value);
                        q.setParameter(paramName, paramValue);

                        break;
                    }
                }

            }
        }
    }

    /**
     * @param queryString
     * @param q
     */
    public static void setQueryParameters(String queryString, HashMap<String, String> paramsMap, Query q) {
        KunderaQuery kq = ((QueryImpl) q).getKunderaQuery();
        Set<Parameter<?>> parameters = kq.getParameters();
        for (String paramName : paramsMap.keySet()) {
            String value = paramsMap.get(paramName);

            if (StringUtils.isNumeric(paramName)) {
                for (Parameter param : parameters) {
                    if (param.getPosition() == Integer.parseInt(paramName)) {
                        Class<?> paramClass = param.getParameterType();
                        PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(paramClass);
                        Object paramValue = accessor.fromString(paramClass, value);
                        q.setParameter(Integer.parseInt(paramName), paramValue);
                        break;
                    }
                }
            } else {
                for (Parameter param : parameters) {
                    if (param.getName().equals(paramName)) {
                        Class<?> paramClass = param.getParameterType();
                        PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(paramClass);
                        Object paramValue = accessor.fromString(paramClass, value);
                        if (paramName.equalsIgnoreCase("firstResult")) {
                            q.setFirstResult(Integer.parseInt((String) paramValue));
                        } else if (paramName.equalsIgnoreCase("maxResult")) {
                            q.setMaxResults(Integer.parseInt((String) paramValue));
                        } else {
                            q.setParameter(paramName, paramValue);
                        }
                        break;
                    }
                }

            }
        }
    }

    public static boolean isValidQuery(String queryString, String httpMethod) {
        if (queryString == null || httpMethod == null) {
            return false;
        }
        queryString = queryString.trim();
        if (queryString.length() < 6)
            return false;
        String firstKeyword = queryString.substring(0, 6);
        String allowedKeyword = httpMethods.get(httpMethod);

        if (allowedKeyword != null && firstKeyword.equalsIgnoreCase(allowedKeyword)) {
            return true;
        } else {
            return false;
        }
    }
}
