package com.impetus.kundera.configure.schema;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableInfo {
	/** The log. */
	private static Logger log = LoggerFactory.getLogger(TableInfo.class);
	private String tableName;
	private List<ColumnInfo> columnMetadatas;
	private String tableIdType;
	private String type;
	private SchemaAction action;
	private List<EmbeddedColumnInfo> embeddedColumnMetadatas;
	private boolean isIndexable ;

	public TableInfo() {
	}

	@Override
	public boolean equals(Object obj) {
		boolean result = false;
		if (obj == null) {
			result = false;
		}
		if (getClass() != obj.getClass()) {
			result = false;
		}
		TableInfo tableInfo = (TableInfo) obj;

		if (this.tableName != null
				&& this.tableName.equals(tableInfo.tableName)
				&& this.type.equals(tableInfo.type)
				&& this.tableIdType.equals(tableInfo.tableIdType)) {

			result = true;
		}
		return result;
	}

	/**
	 * @return the action
	 */
	public SchemaAction getAction() {
		return action;
	}

	/**
	 * @param action
	 *            the action to set
	 */
	public void setAction(SchemaAction action) {
		this.action = action;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName
	 *            the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * @return the tableIdType
	 */
	public String getTableIdType() {
		return tableIdType;
	}

	/**
	 * @param tableIdType
	 *            the tableIdType to set
	 */
	public void setTableIdType(String tableIdType) {
		this.tableIdType = tableIdType;
	}

	/**
	 * @return the columnMetadatas
	 */
	public List<ColumnInfo> getColumnMetadatas() {
		return columnMetadatas;
	}

	/**
	 * @param columnMetadatas
	 *            the columnMetadatas to set
	 */
	public void setColumnMetadatas(List<ColumnInfo> columnMetadatas) {
		this.columnMetadatas = columnMetadatas;
	}

	/**
	 * @return the embeddedColumnMetadatas
	 */
	public List<EmbeddedColumnInfo> getEmbeddedColumnMetadatas() {
		return embeddedColumnMetadatas;
	}

	/**
	 * @param embeddedColumnMetadatas
	 *            the embeddedColumnMetadatas to set
	 */
	public void setEmbeddedColumnMetadatas(
			List<EmbeddedColumnInfo> embeddedColumnMetadatas) {
		this.embeddedColumnMetadatas = embeddedColumnMetadatas;
	}

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @return the isIndexable
	 */
	public boolean isIndexable() {
		return isIndexable;
	}

	/**
	 * @param isIndexable the isIndexable to set
	 */
	public void setIndexable(boolean isIndexable) {
		this.isIndexable = isIndexable;
	}

	/**
	 * @param type
	 *            the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	public enum SchemaAction {
		createdrop, create, update, validate;

		/**
		 * 
		 * @param name
		 * @return
		 */
		public static SchemaAction instanceOf(String name) {
			if (name.equalsIgnoreCase("create-drop")) {
				return SchemaAction.createdrop;
			}
			return SchemaAction.valueOf(SchemaAction.class, name);
		}
	}

}
