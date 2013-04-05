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

import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.util.kvlite.KVLite;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */
@RunWith(Suite.class)
@SuiteClasses({ BasicKVStoreTest.class, EntityPersistenceKVStoreTest.class })
public class AllTests {

    /** The kv lite. */
    protected static KVLite kvLite;

    //@BeforeClass
    public static void setUpClass() {
        System.out.println("Master setup");
        String storeName = "kvstore";
        String hostName = "localhost";
        String hostPort = "5000";

        try {
            KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
        } catch (Exception e) {
            try {
                delete(new File("./kvroot"));
            } catch (IOException e3) {
                e3.printStackTrace();
            }
            try {
                delete(new File("./lucene"));
            } catch (IOException e3) {
                e3.printStackTrace();
            }
            // server not found running, try running it
            
            //new KVLite("./kvroot", "kvstore", 5000, 5001, "localhost", "5010,5020", servicePortRange, 1, mountPoint, false);
            try {
                kvLite.start();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
            waitForKVStoreToStart();
        }

    }

    //@AfterClass
    public static void tearDownClass() {
        System.out.println("Master tearDown");
        if (kvLite != null) {
            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            kvLite.shutdownStore(true);
        }
    }

    /**
     * Delete kvstore.
     * 
     * @param f
     *            the f
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    /**
     * Wait for kv store to start.
     */
    private static void waitForKVStoreToStart() {
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
