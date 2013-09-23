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
package com.impetus.kundera.tests.cli;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Provides utility methods for all Kundera Examples test cases
 * 
 * @author amresh.singh
 */
public class CleanupUtilities
{
    /** The log. */
    private static Logger log = LoggerFactory.getLogger(CleanupUtilities.class);

    public static void cleanLuceneDirectory(String persistenceUnit)
    {
        PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        if (puMetadata != null)
        {
            String luceneDir = puMetadata.getProperty(PersistenceProperties.KUNDERA_INDEX_HOME_DIR);
            if (luceneDir != null && luceneDir.length() > 0)
            {
                log.debug("Cleaning up lucene folder " + luceneDir);
                File directory = new File(luceneDir);
                // Get all files in directory
                File[] files = directory.listFiles();
                for (File file : files)
                {
                    // Delete each file
                    if (!file.delete())
                    {
                        // Failed to delete file
                        log.info("Failed to delete " + file);
                    }
                }
            }
        }

    }

}
