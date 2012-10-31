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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.persistence.EntityManager;
import javax.persistence.Parameter;
import javax.persistence.Query;

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryImpl;

/**
 * @author amresh
 * 
 */
public class EntityUtils
{

    /**
     * @param entityClassName
     * @param em
     * @return
     */
    public static Class<?> getEntityClass(String entityClassName, EntityManager em)
    {
        MetamodelImpl metamodel = (MetamodelImpl) em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);
        return entityClass;
    }
    
    public static String getQueryPart(String fullQueryString)
    {
        if(fullQueryString.contains("?"))
        {   
            return fullQueryString.substring(0, fullQueryString.indexOf("?"));
        }
        else
        {
            return fullQueryString;
        }        
    }
    
    public static String getParameterPart(String fullQueryString)
    {
        if(fullQueryString.contains("?"))            
        {     
            return fullQueryString.substring(fullQueryString.indexOf("?") + 1, fullQueryString.length()); 
        }
        else
        {
            return "";
        }
    }
    
    /**
     * @param queryString
     * @param q
     */
    public static void setQueryParameters(String queryString, String parameterString, Query q)
    {
        Map<String, String> paramsMap = new HashMap<String, String>();               
        
        StringTokenizer st = new StringTokenizer(parameterString, "&");
        while(st.hasMoreTokens()) {
            String element = st.nextToken();
            paramsMap.put(element.substring(0, element.indexOf("=")), element.substring(element.indexOf("=") + 1, element.length()));
        }          

        for(String paramName : paramsMap.keySet()) {
            String value = paramsMap.get(paramName);
            
            KunderaQuery kq = ((QueryImpl) q).getKunderaQuery();
            Set<Parameter<?>> parameters = kq.getParameters();
            
            if(StringUtils.isNumeric(paramName))
            {
                for(Parameter param : parameters)
                {
                    if(param.getPosition() == Integer.parseInt(paramName))
                    {
                        Class<?> paramClass = param.getParameterType();
                        PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(paramClass);
                        Object paramValue = accessor.fromString(paramClass, value);                        
                        q.setParameter(Integer.parseInt(paramName), paramValue);
                        break;
                    }
                }               
            }
            else
            {
                for(Parameter param : parameters)
                {
                    if(param.getName().equals(paramName))
                    {
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
}
