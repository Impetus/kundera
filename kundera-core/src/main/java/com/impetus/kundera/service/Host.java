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
package com.impetus.kundera.service;

import com.impetus.kundera.service.policy.LoadBalancingPolicy;

/**
 * Host interface all module will implement it in order to provide
 * {@link LoadBalancingPolicy }.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public interface Host
{
    /**
     * 
     * @return host.
     */
    String getHost();

    /**
     * 
     * @return port.
     */
    int getPort();

    /**
     * @return the maxTotal
     */
    int getMaxTotal();

    /**
     * @return the maxActive
     */
    int getMaxActive();

    /**
     * @return the maxIdle
     */
    int getMaxIdle();

    /**
     * @return the minIdle
     */
    int getMinIdle();

    /**
     * 
     * @return true/false.
     */
    public boolean isRetryHost();

    /**
     * @return the Username
     */
    String getUser();

    /**
     * @return the Username
     */
    String getPassword();
}
