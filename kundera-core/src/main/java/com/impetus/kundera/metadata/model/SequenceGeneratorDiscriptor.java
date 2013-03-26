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
package com.impetus.kundera.metadata.model;

import javax.persistence.SequenceGenerator;

/**
 * SequenceGeneratorDiscriptor class holds the information about sequence
 * generator.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class SequenceGeneratorDiscriptor
{
    private static final int default_initial_value = 1;

    private static final String default_sequence_name = "sequence_name";

    private static final int default_allocation_size = 50;

    private int initialValue;

    private int allocationSize;

    private String sequenceName;

    private String schemaName;

    private String catalog;

    public SequenceGeneratorDiscriptor(SequenceGenerator sequenceGenerator, String defaultSchemaName)
    {
        this.initialValue = sequenceGenerator.initialValue();
        this.allocationSize = sequenceGenerator.allocationSize();
        this.sequenceName = sequenceGenerator.sequenceName().isEmpty() ? default_sequence_name : sequenceGenerator
                .sequenceName();
        this.schemaName = sequenceGenerator.schema().isEmpty() ? defaultSchemaName : sequenceGenerator.schema();
    }

    public SequenceGeneratorDiscriptor(String defaultSchemaName)
    {
        this.initialValue = default_initial_value;
        this.allocationSize = default_allocation_size;
        this.sequenceName = default_sequence_name;
        this.schemaName = defaultSchemaName;
    }

    /**
     * @return the initialValue
     */
    public int getInitialValue()
    {
        return initialValue;
    }

    /**
     * @return the allocationSize
     */
    public int getAllocationSize()
    {
        return allocationSize;
    }

    /**
     * @return the sequenceName
     */
    public String getSequenceName()
    {
        return sequenceName;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * @return the catalog
     */
    public String getCatalog()
    {
        return catalog;
    }

}
