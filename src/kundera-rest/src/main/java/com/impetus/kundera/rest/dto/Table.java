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
package com.impetus.kundera.rest.dto;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Holds Table information
 * 
 * @author amresh.singh
 */

@XmlRootElement
public class Table
{
    private String tableName;

    private String entityClassName;
    
    private String simpleEntityClassName;

    public String getSimpleEntityClassName() {
		return simpleEntityClassName;
	}

	public void setSimpleEntityClassName(String simpleEntityClassName) {
		this.simpleEntityClassName = simpleEntityClassName;
	}

	/**
     * @return the tableName
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * @param tableName
     *            the tableName to set
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    /**
     * @return the entityClassName
     */
    public String getEntityClassName()
    {
        return entityClassName;
    }

    /**
     * @param entityClassName
     *            the entityClassName to set
     */
    public void setEntityClassName(String entityClassName)
    {
        this.entityClassName = entityClassName;
    }

}
