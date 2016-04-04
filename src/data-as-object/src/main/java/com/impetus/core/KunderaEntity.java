/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.core;

import java.util.List;

/**
 * The Interface KunderaEntity.
 *
 * @param <T>
 *            the generic type
 * @param <K>
 *            the key type
 */
public interface KunderaEntity<T, K>
{

    /**
     * Find.
     *
     * @param key
     *            the key
     * @return the t
     */
    T find(K key);

    /**
     * Save.
     */
    void save();

    /**
     * Update.
     */
    void update();

    /**
     * Delete.
     */
    void delete();

    /**
     * Query.
     *
     * @param query
     *            the query
     * @return the list
     */
    List<T> query(String query);

    /**
     * Query.
     *
     * @param query
     *            the query
     * @param type
     *            the type
     * @return the list
     */
    List<T> query(String query, QueryType type);

    /**
     * Left join.
     *
     * @param clazz
     *            the clazz
     * @param joinColumn
     *            the join column
     * @param columnTobeFetched
     *            the column tobe fetched
     * @return the list
     */
    // List<T> leftJoin(Class clazz, String joinColumn, String...
    // columnTobeFetched);

}
