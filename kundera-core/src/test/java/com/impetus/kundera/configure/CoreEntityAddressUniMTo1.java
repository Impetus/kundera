package com.impetus.kundera.configure;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "CoreEntityAddressUniMTo1", schema = "KunderaCoreExmples@cassandra")
public class CoreEntityAddressUniMTo1 {
	@Id
	@Column(name = "ADDRESS_ID")
	private String addressId;

	@Column(name = "STREET")
	private String street;

	public String getAddressId() {
		return addressId;
	}

	public void setAddressId(String addressId) {
		this.addressId = addressId;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}
}
