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
package com.impetus.kundera.graph;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;

import com.impetus.kundera.metadata.model.Relation;

/**
 * Holds link meta data for a unidirectional directed link from source node to
 * target node.
 * 
 * @author amresh.singh
 */
public class NodeLink
{
    // All possible node link properties
    public enum LinkProperty
    {
        LINK_NAME, LINK_VALUE, IS_SHARED_BY_PRIMARY_KEY, IS_BIDIRECTIONAL, IS_RELATED_VIA_JOIN_TABLE, PROPERTY, BIDIRECTIONAL_PROPERTY, CASCADE, JOIN_TABLE_METADATA
        // Add more if required
    };

    private String sourceNodeId;

    private String targetNodeId;

    // Multiplicity of relationship
    private Relation.ForeignKey multiplicity;

    // Contains all properties for this link
    private Map<LinkProperty, Object> linkProperties;

    public NodeLink()
    {

    }

    public NodeLink(String sourceNodeId, String targetNodeId)
    {
        this.sourceNodeId = sourceNodeId;
        this.targetNodeId = targetNodeId;
    }

    /**
     * @return the sourceNodeId
     */
    public String getSourceNodeId()
    {
        return sourceNodeId;
    }

    /**
     * @param sourceNodeId
     *            the sourceNodeId to set
     */
    public void setSourceNodeId(String sourceNodeId)
    {
        this.sourceNodeId = sourceNodeId;
    }

    /**
     * @return the targetNodeId
     */
    public String getTargetNodeId()
    {
        return targetNodeId;
    }

    /**
     * @param targetNodeId
     *            the targetNodeId to set
     */
    public void setTargetNodeId(String targetNodeId)
    {
        this.targetNodeId = targetNodeId;
    }

    /**
     * @return the multiplicity
     */
    public Relation.ForeignKey getMultiplicity()
    {
        return multiplicity;
    }

    /**
     * @param multiplicity
     *            the multiplicity to set
     */
    public void setMultiplicity(Relation.ForeignKey multiplicity)
    {
        this.multiplicity = multiplicity;
    }

    /**
     * @return the linkProperties
     */
    public Map<LinkProperty, Object> getLinkProperties()
    {
        return linkProperties;
    }

    /**
     * @param linkProperties
     *            the linkProperties to set
     */
    public void setLinkProperties(Map<LinkProperty, Object> linkProperties)
    {
        this.linkProperties = linkProperties;
    }

    /**
     * @return the linkProperties
     */
    public Object getLinkProperty(LinkProperty name)
    {
        if (linkProperties == null || linkProperties.isEmpty())
        {
            throw new IllegalStateException("Link properties not initialized");
        }

        return linkProperties.get(name);
    }

    public void addLinkProperty(LinkProperty name, Object propertyValue)
    {
        if (linkProperties == null)
        {
            linkProperties = new HashMap<NodeLink.LinkProperty, Object>();
        }

        linkProperties.put(name, propertyValue);
    }

    @Override
    public int hashCode()
    {
        int n = getSourceNodeId().hashCode() * getTargetNodeId().hashCode();
        return n;

        // return new
        // HashCodeBuilder().append(getSourceNodeId()).append(getTargetNodeId()).hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (!(obj instanceof NodeLink))
        {
            return false;
        }

        NodeLink targetNodeLink = (NodeLink) obj;

        return new EqualsBuilder().append(getSourceNodeId(), targetNodeLink.getSourceNodeId())
                .append(getTargetNodeId(), targetNodeLink.getTargetNodeId()).isEquals();
        // return getSourceNodeId().equals(targetNodeLink.getSourceNodeId())
        // && getTargetNodeId().equals(targetNodeLink.getTargetNodeId());
    }

    @Override
    public String toString()
    {
        return sourceNodeId + "---(" + multiplicity + ")--->" + targetNodeId;
    }

}
