/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.graph;

import java.lang.reflect.Field;

import javax.persistence.GeneratedValue;

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.Constants;
import com.impetus.kundera.utils.NumericUtils;

/**
 * Provides utility methods for object graph
 * 
 * @author amresh.singh
 */
public class ObjectGraphUtils
{

    /**
     * 
     * @param pk
     * @param objectClass
     * @return
     */
    public static String getNodeId(Object pk, Class<?> objectClass)
    {
        StringBuffer strBuffer = new StringBuffer(objectClass.getName());
        strBuffer.append(Constants.NODE_ID_SEPARATOR);
        strBuffer.append(pk);
        return strBuffer.toString();
    }

    /**
     * 
     * @param nodeId
     * @return
     */
    public static Object getEntityId(String nodeId)
    {
        return nodeId.substring(nodeId.indexOf(Constants.NODE_ID_SEPARATOR) + 1, nodeId.length());
    }

    /**
     * Validates and set id, in case not set and intended for auto generation.
     * 
     * @param idField
     *            id field
     * @param idValue
     *            value of id attribute.
     * @return returns true if id is not set and @GeneratedValue annotation is
     *         present. Else false.
     */
    public static boolean onAutoGenerateId(Field idField, Object idValue)
    {
        if (idField.isAnnotationPresent(GeneratedValue.class))
        {
            return !isIdSet(idValue, idField);
        }

        return false;
    }

    /**
     * 
     * @param id
     * @param idField
     * @return
     */
    private static boolean isIdSet(Object id, Field idField)
    {
        // return true, if it is non blank and not zero in case of numeric
        // value.

        if (id != null)
        {
            return !(NumericUtils.checkIfZero(id.toString(), idField.getType()) || (StringUtils
                    .isNumeric(id.toString()) && StringUtils.isBlank(id.toString())));
        }
        return false;
    }

}
