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
package com.impetus.kundera.service.policy;

import java.util.Collection;

/**
 * Abstract class leastActiveBalancingPolicy, all client factory will have a class which will extends it
 * in order to support least active load.
 * balancing policy.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public abstract class LeastActiveBalancingPolicy implements LoadBalancingPolicy
{

    /**
     * @return pool object for host which has least active connections.
     * 
     */
    @Override
    public abstract Object getPool(Collection<Object> pools);
}
