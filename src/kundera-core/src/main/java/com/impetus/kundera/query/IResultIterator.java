/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.query;

import java.util.Iterator;
import java.util.List;

/**
 * @author vivek.mishra
 * 
 * Iterator interface extends {@link Iterator}. Databases(e.g. Cassandra,HBase etc) implementation to implement {@link IResultIterator} 
 * for pagination/scrolling support. 
 *
 */
public interface IResultIterator<E> extends Iterator<E>
{

    /**
     * Returns next chunk of records. If no next chunk is available, will return an empty list.
     * 
     * @param chunkSize   no of records to be fetched.
     * 
     * @return collection of E entity.
     */
    List<E> next(int chunkSize);
}
