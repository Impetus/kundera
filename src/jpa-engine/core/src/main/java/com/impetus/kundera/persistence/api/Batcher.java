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
package com.impetus.kundera.persistence.api;

import com.impetus.kundera.graph.Node;

/**
 * API to handler batch operations.
 * 
 * @author vivek.mishra
 * 
 */
public interface Batcher
{

    /**
     * Adds node to batch collection.
     * 
     * @param node
     *            data node.
     */
    void addBatch(Node node);

    /**
     * executes batch.
     * 
     * @return returns number of records persisted/update via batch.
     */
    int executeBatch();

    /**
     * Returns batch size
     * 
     * @return batch size as integer
     */
    int getBatchSize();

    /**
     * In case user asked for
     */
    void clear();

}
