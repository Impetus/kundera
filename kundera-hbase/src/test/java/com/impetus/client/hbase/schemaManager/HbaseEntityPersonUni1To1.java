package com.impetus.client.hbase.schemaManager;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

@Entity
@Table(name = "HbaseEntityPersonUni1To1", schema = "KunderaHbaseExamples@hbase")
public class HbaseEntityPersonUni1To1 {

	@Id
	@Column(name = "PERSON_ID")
	private String personId;

	@Column(name = "PERSON_NAME")
	private String personName;

	@Column(name = "AGE")
	private short age;

	@Embedded
	private HbasePersonalData personalData;

	@OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
	@JoinColumn(name = "ADDRESS_ID")
	private HbaseEntityAddressUni1To1 address;

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

	/**
	 * @return the address
	 */
	public HbaseEntityAddressUni1To1 getAddress() {
		return address;
	}

	/**
	 * @param address
	 *            the address to set
	 */
	public void setAddress(HbaseEntityAddressUni1To1 address) {
		this.address = address;
	}

}
