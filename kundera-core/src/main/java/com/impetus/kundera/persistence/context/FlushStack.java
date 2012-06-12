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

import java.util.Stack;

import com.impetus.kundera.graph.Node;

/**
 * Stack containing all Nodes to be flushed to persistence store
 * 
 * @author amresh.singh
 */
public class FlushStack extends Stack<Node>
{
    public FlushStack()
    {
        super();
    }

    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("Flush Stack(From top to bottom):\n");
        sb.append("--------------------------------------------\n");
        for (int i = elementCount - 1; i >= 0; i--)
        {
            sb.append("|").append(get(i)).append("\t|\n");
        }
        sb.append("--------------------------------------------");
        return sb.toString();
    }

    @Override
    public Node push(Node node)
    {
        int i = indexOf(node);
        if (i == -1)
        {
            super.push(node);
        }
        else
        {
            remove(i);
            insertElementAt(node, i);
        }
        return node;
    }
}
