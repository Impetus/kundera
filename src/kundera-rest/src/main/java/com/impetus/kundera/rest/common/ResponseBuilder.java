/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Utility methods for handling entities passed in REST request
 * 
 * @author chhavi.gangwal
 * 
 */
public class ResponseBuilder {

    public static Map<String, String> httpMethods = new HashMap<String, String>();

    private static Logger log = LoggerFactory.getLogger(ResponseBuilder.class);

    /**
     * @param entityClassName
     * @param em
     * @return
     */
    public static String buildOutput(Class<?> entityClass, EntityMetadata entityMetadata, Object output) {
        StringBuilder sb = new StringBuilder("'");
        sb.append("{\"").append(entityClass.getSimpleName().toLowerCase()).append("\":").append(output)
            .append(",\"entityClassName\":\"").append(entityMetadata.getEntityClazz().getSimpleName())
            .append("\",\"id\":\"" + entityMetadata.getIdAttribute().getName()).append("\"}").append("'");
        return sb.toString();
    }
    
 
    /**
     * @param output
     * @param literal
     * @return
     */
    public static String buildOutput(String output, String literal) {
        StringBuilder sb = new StringBuilder(literal);
        sb.append(output).append(literal);
        return sb.toString();
    }


   

}
