/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.index;

import java.util.List;
import java.util.Properties;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * @author Kuldeep.Mishra
 * 
 */
@XmlRootElement
public class IndexerProperties
{

    private List<Node> nodes;

    @XmlRootElement
    public static class Node
    {
        private Properties properties;

        public Properties getProperties()
        {
            return properties;
        }

        public void setProperties(Properties properties)
        {
            this.properties = properties;
        }
    }

    @XmlElement
    public List<Node> getNodes()
    {
        return nodes;
    }

    public void setNodes(List<Node> nodes)
    {
        this.nodes = nodes;
    }
}