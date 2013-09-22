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

package com.impetus.kundera.persistence;

/**
 * TransactionBinder interface. If underlying database provides in-built
 * transaction support, client has to implement this interface and bind
 * transaction resource with client.
 * 
 * @author vivek.mishra
 * 
 */
public interface TransactionBinder
{

    /**
     * Binds a transaction resource with client instance. Any subsequent CRUD
     * calls will use this transaction resource to mark bind within already
     * running transaction boundary. TransactionResource is responsible to bind
     * and provide connection instance with for subsequent commit/rollback.
     * 
     * @param resource
     *            transactional resource
     */
    void bind(TransactionResource resource);
}
