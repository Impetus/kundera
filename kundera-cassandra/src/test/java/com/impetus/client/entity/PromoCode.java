/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.entity;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class PromoCode {

	@Column(name = "promoCode_id")
	private String promoCodeId;

	@Column(name = "promoCode_name")
	private String promoCodeName;

	public String getPromoCodeId() {
		return promoCodeId;
	}

	public void setPromoCodeId(String promoCodeId) {
		this.promoCodeId = promoCodeId;
	}

	public String getPromoCodeName() {
		return promoCodeName;
	}

	public void setPromoCodeName(String promoCodeName) {
		this.promoCodeName = promoCodeName;
	}

}
