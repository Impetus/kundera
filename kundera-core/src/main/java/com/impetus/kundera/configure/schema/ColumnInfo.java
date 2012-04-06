package com.impetus.kundera.configure.schema;

public class ColumnInfo {
	private boolean isIndexable = false;
	private String columnName;
	private String type;

	// public ColumnInfo(Column column, List<PropertyIndex> indexs) {
	// setType(column.getField().getType().toString());
	// for (PropertyIndex index : indexs) {
	// if (column.getName().equals(index.getName())) {
	// setIndexable(true);
	// }
	// }
	// }

	public ColumnInfo() {

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
		ColumnInfo columnInfo = (ColumnInfo) obj;

		if (this.columnName != null
				&& this.columnName.equals(columnInfo.columnName)) {

			result = true;
		}
		return result;
	}

	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * @param columnName
	 *            the columnName to set
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
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
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type
	 *            the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

}
