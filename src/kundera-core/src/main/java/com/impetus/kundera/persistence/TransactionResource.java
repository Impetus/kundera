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
 * TransactionResource interface will delegate begin/commit/rollback to client
 * and each client will implement this interface provided transaction support is
 * required for corresponding database. If underlying database supports
 * transaction then it will
 * 
 * @author vivek.mishra
 * 
 */
public interface TransactionResource
{

    /**
     * On begin transactions.
     */
    void onBegin();

    /**
     * On commit transactions.
     */
    void onCommit();

    /**
     * On rollback transactions.
     */
    void onRollback();

    /**
     * On intermediate flush, when explicitly flush is invoked by em.flush()!
     */
    void onFlush();

    /**
     * On prepare for two phase commit.
     * 
     * @return response, returns YES if it is ready for commit
     */
    Response prepare();

    /**
     * Returns true if transaction is active else false.
     * 
     * @return boolean true.
     */
    boolean isActive();

    /**
     * 
     * Response enum
     * 
     */
    enum Response
    {
        YES, NO;
    }
}
