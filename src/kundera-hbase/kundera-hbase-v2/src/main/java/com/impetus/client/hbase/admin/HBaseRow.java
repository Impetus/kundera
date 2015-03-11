/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.admin;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Pragalbh Garg
 *
 */
public class HBaseRow
{
    private Object rowKey;
    private List<HBaseCell> rowCells = new ArrayList<HBaseCell>();
    
    /**
     * Instantiates a new h base row.
     * 
     * @param rowKey
     *            the row key
     * @param rowCells
     *            the row cells
     */
    public HBaseRow(Object rowKey, List<HBaseCell> rowCells){
        this.setRowKey(rowKey);
        this.setRowCells(rowCells);
    }
    
    /**
     * Instantiates a new h base row.
     */
    public HBaseRow()
    {
    }

    /**
     * Adds the cell.
     * 
     * @param hbaseCell
     *            the hbase cell
     */
    public void addCell(HBaseCell hbaseCell){
        this.rowCells.add(hbaseCell);
    }

    /**
     * Gets the row key.
     * 
     * @return the row key
     */
    public Object getRowKey()
    {
        return rowKey;
    }

    /**
     * Sets the row key.
     * 
     * @param rowKey
     *            the new row key
     */
    public void setRowKey(Object rowKey)
    {
        this.rowKey = rowKey;
    }

    /**
     * Gets the row cells.
     * 
     * @return the row cells
     */
    public List<HBaseCell> getRowCells()
    {
        return rowCells;
    }

    /**
     * Sets the row cells.
     * 
     * @param rowCells
     *            the new row cells
     */
    public void setRowCells(List<HBaseCell> rowCells)
    {
        this.rowCells = rowCells;
    }

    /**
     * Adds the cells.
     * 
     * @param rowCells
     *            the row cells
     */
    public void addCells(List<HBaseCell> rowCells)
    {
        this.rowCells.addAll(rowCells);
        
    }
    
    

}
