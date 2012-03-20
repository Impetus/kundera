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
package com.impetus.kundera.persistence.context;

import java.util.List;
import java.util.Map;

import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.NodeLink;
import com.impetus.kundera.metadata.model.Relation;

/**
 * Provides utility methods for managing Flush Stack.
 * @author amresh.singh
 */
public class FlushStackManager
{
    public void buildFlushStack(PersistenceCache pc) {
        
        //Process Main cache
        MainCache mainCache = (MainCache)pc.getMainCache();
        
        List<Node> headNodes = mainCache.getHeadNodes();
        
        for(Node headNode : headNodes) {
            addNodesToFlushStack(pc, headNode);
        }
        
    }
    
    /**
     * Pushes <code>node</code> and its descendants recursively to flush stack 
     * residing into persistence cache
     * @param pc
     * @param node
     */
    private void addNodesToFlushStack(PersistenceCache pc, Node node) {
        FlushStack flushStack = pc.getFlushStack();
        MainCache mainCache = (MainCache)pc.getMainCache();
        Map<String, Node> nodeMappings = mainCache.getNodeMappings();  
        
        Map<NodeLink, Node> children = node.getChildren();        
        
        for(NodeLink nodeLink : children.keySet()) {
            Relation.ForeignKey multiplicity = nodeLink.getMultiplicity();
            
            switch(multiplicity) {
                case ONE_TO_ONE:
                    node.setTraversed(true);
                    flushStack.push(node);
                    addNodesToFlushStack(pc, nodeMappings.get(nodeLink.getTargetNodeId()));
                    
                case ONE_TO_MANY:
                    
                case MANY_TO_ONE:
                    
                case MANY_TO_MANY:
                    
                default:
                    
            }
            
            
        }
        
        
    }
}
