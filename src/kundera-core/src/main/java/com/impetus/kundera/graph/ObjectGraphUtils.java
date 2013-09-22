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

import com.impetus.kundera.Constants;

/**
 * Provides utility methods for object graph
 * 
 * @author amresh.singh
 */
public class ObjectGraphUtils
{
    /*
     * public static String getNodeId(Object pk, Object nodeData) { StringBuffer
     * strBuffer = new StringBuffer(nodeData.getClass().getName());
     * strBuffer.append(Constants.NODE_ID_SEPARATOR); strBuffer.append(pk);
     * return strBuffer.toString(); }
     */

    public static String getNodeId(Object pk, Class<?> objectClass)
    {
        StringBuffer strBuffer = new StringBuffer(objectClass.getName());
        strBuffer.append(Constants.NODE_ID_SEPARATOR);
        strBuffer.append(pk);
        return strBuffer.toString();
    }

    public static Object getEntityId(String nodeId)
    {
        return nodeId.substring(nodeId.indexOf(Constants.NODE_ID_SEPARATOR) + 1, nodeId.length());
    }
}
