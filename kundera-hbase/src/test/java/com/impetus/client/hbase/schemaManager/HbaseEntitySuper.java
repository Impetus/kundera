package com.impetus.client.hbase.schemaManager;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "HbaseEntitySuper", schema = "KunderaHbaseExamples@hbase")
public class HbaseEntitySuper {

	@Id
	@Column(name = "PERSON_ID")
	private String personId;

	@Column(name = "PERSON_NAME")
	private String personName;

	@Column(name = "AGE")
	private short age;

	@Embedded
	private HbasePersonalData personalData;

	public String getPersonId() {
		return personId;
	}

	public String getPersonName() {
		return personName;
	}

	public void setPersonName(String personName) {
		this.personName = personName;
	}

	public void setPersonId(String personId) {
		this.personId = personId;
	}

	/**
	 * @return the age
	 */
	public short getAge() {
		return age;
	}

	/**
	 * @param age
	 *            the age to set
	 */
	public void setAge(short age) {
		this.age = age;
	}

	/**
	 * @return the personalData
	 */
	public HbasePersonalData getPersonalData() {
		return personalData;
	}

	/**
	 * @param personalData
	 *            the personalData to set
	 */
	public void setPersonalData(HbasePersonalData personalData) {
		this.personalData = personalData;
	}

}
