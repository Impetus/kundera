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
package com.impetus.client.mongodb.index;

/**
 * Helper class for Mongo DB Indexing 
 * @author amresh.singh
 */
public class MongoDBIndexHelper
{
    /**
     * @param indexType
     * @param clazz
     * @return
     */
    public static String getIndexType(String indexType, Class clazz)
    {
        // TODO validation for indexType and attribute type

        if (indexType != null)
        {
            if (indexType.equals(IndexType.ASC))
            {
                return IndexType.findByValue(IndexType.ASC);
            }
            else if (indexType.equals(IndexType.DSC))
            {
                return IndexType.findByValue(IndexType.DSC);
            }
            else if (indexType.equals(IndexType.GEO2D))
            {
                return IndexType.findByValue(IndexType.GEO2D);
            }
        }
        return IndexType.findByValue(IndexType.ASC);
    }
}
