/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
/*
 * author: karthikp.manchala
 */
package com.impetus.kundera.client.cassandra.dsdriver;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * The Class CustomRetry.
 */
public class CustomRetry implements RetryPolicy
{

    /** The Constant INSTANCE. */
    public static final CustomRetry INSTANCE = new CustomRetry();

    /**
     * Gets the single instance of CustomRetry.
     * 
     * @return single instance of CustomRetry
     */
    public static CustomRetry getInstance()
    {
        return INSTANCE;
    }

    /**
     * Instantiates a new custom retry.
     */
    private CustomRetry()
    {

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.datastax.driver.core.policies.RetryPolicy#onReadTimeout(com.datastax
     * .driver.core.Statement, com.datastax.driver.core.ConsistencyLevel, int,
     * int, boolean, int)
     */
    @Override
    public RetryDecision onReadTimeout(Statement arg0, ConsistencyLevel arg1, int arg2, int arg3, boolean arg4, int arg5)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.datastax.driver.core.policies.RetryPolicy#onUnavailable(com.datastax
     * .driver.core.Statement, com.datastax.driver.core.ConsistencyLevel, int,
     * int, int)
     */
    @Override
    public RetryDecision onUnavailable(Statement arg0, ConsistencyLevel arg1, int arg2, int arg3, int arg4)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.datastax.driver.core.policies.RetryPolicy#onWriteTimeout(com.datastax
     * .driver.core.Statement, com.datastax.driver.core.ConsistencyLevel,
     * com.datastax.driver.core.WriteType, int, int, int)
     */
    @Override
    public RetryDecision onWriteTimeout(Statement arg0, ConsistencyLevel arg1, WriteType arg2, int arg3, int arg4,
            int arg5)
    {
        return null;
    }

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void init(Cluster arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public RetryDecision onRequestError(Statement arg0, ConsistencyLevel arg1,
			DriverException arg2, int arg3) {
		// TODO Auto-generated method stub
		return null;
	}

}
