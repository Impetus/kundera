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
package com.impetus.kundera.persistence;


/**
 * @author vivek.mishra
 *
 */
 final class AssociationBuilder
{

    
    
    /**
     * Returns lucene based query.
     * @param clazzFieldName       lucene field name for class 
     * @param clazzName            class name
     * @param idFieldName          lucene id field name
     * @param idFieldValue         lucene id field value
     * @return query               lucene query.
     */
    static String getQuery(String clazzFieldName, String clazzName, String idFieldName, String idFieldValue)
    {
        StringBuffer sb = new StringBuffer("+");
        sb.append(clazzFieldName);
        sb.append(":");
        sb.append(clazzName);
        sb.append(" AND ");
        sb.append("+");
        sb.append(idFieldName);
        sb.append(":");
        sb.append(idFieldValue);
        return sb.toString();
    }

}
