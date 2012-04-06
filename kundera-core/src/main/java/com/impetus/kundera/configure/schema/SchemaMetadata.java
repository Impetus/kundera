/**
 * 
 */
package com.impetus.kundera.configure.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author impadmin
 *
 */
public final class SchemaMetadata {
	
	private Map<String, List<TableInfo>> puToSchemaCol;

	/**
	 * @return the puToSchemaCol
	 */
	public Map<String, List<TableInfo>> getPuToSchemaCol() 
	{
		if(puToSchemaCol == null)
		{
			puToSchemaCol = new HashMap<String, List<TableInfo>>();
		}
		return puToSchemaCol;
	}

	/**
	 * @param puToSchemaCol the puToSchemaCol to set
	 */
	public void setPuToSchemaCol(Map<String, List<TableInfo>> puToSchemaCol) {
		this.puToSchemaCol = puToSchemaCol;
	}
	
}
