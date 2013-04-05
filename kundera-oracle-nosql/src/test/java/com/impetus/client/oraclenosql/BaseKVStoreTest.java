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
package com.impetus.client.oraclenosql;



import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.util.kvlite.KVLite;

import org.junit.After;
import org.junit.Before;

/**
 * Abstract Base class for all KVStoreTest.
 * @author amresh.singh
 */
public abstract class BaseKVStoreTest {


    
    /** The store. */
    protected KVStore store;

    /**
     * Instantiates a new base kv store test.
     */
    public BaseKVStoreTest() {
        super();
    }

    /**
     * Setup.
     *
     * @throws InterruptedException the interrupted exception
     */
    //@Before
    public void setup() throws InterruptedException {
        String storeName = "kvstore";
        String hostName = "localhost";
        String hostPort = "5000";

        try {
            store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
        } catch (Exception e) {
            //e.printStackTrace();
        }

    }

   

}