package com.impetus.kundera.index;

import java.util.List;
import java.util.Properties;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

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
