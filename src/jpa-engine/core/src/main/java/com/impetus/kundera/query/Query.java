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

import java.util.Iterator;

/**
 * Query Interface, clients query class must implement these method in order to
 * support pagination.
 * 
 * @author Kuldeep.Mishra
 */
public interface Query
{
    /**
     * To set fetch size for query.
     */
    void setFetchSize(Integer fetchsize);

    /**
     * 
     * @return fetch size value.
     */
    Integer getFetchSize();

    /**
     * Reinstate .
     */
    void close();

    /**
     * add support to enable TTL for UPDATE/INSERT/DELETE queries.
     */
    void applyTTL(int ttlInSeconds);

    /**
     * Iterates over result.
     * 
     * @return
     */
    <E> Iterator<E> iterate();

}