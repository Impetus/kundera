package com.impetus.kundera.configure.schema;

import java.util.List;

public class EmbeddedColumnInfo {

	private String embeddedColumnName;
	private List<ColumnInfo> columns;

	/**
	 * @return the embeddedColumnName
	 */
	public String getEmbeddedColumnName() {
		return embeddedColumnName;
	}

	/**
	 * @param embeddedColumnName
	 *            the embeddedColumnName to set
	 */
	public void setEmbeddedColumnName(String embeddedColumnName) {
		this.embeddedColumnName = embeddedColumnName;
	}

	/**
	 * @return the columns
	 */
	public List<ColumnInfo> getColumns() {
		return columns;
	}

	/**
	 * @param columns
	 *            the columns to set
	 */
	public void setColumns(List<ColumnInfo> columns) {
		this.columns = columns;
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
		EmbeddedColumnInfo embeddedColumnInfo = (EmbeddedColumnInfo) obj;

		if (this.embeddedColumnName != null
				&& this.embeddedColumnName
						.equals(embeddedColumnInfo.embeddedColumnName)) {

			result = true;
		}
		return result;
	}

}
