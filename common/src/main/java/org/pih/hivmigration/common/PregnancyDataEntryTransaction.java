package org.pih.hivmigration.common;

import java.util.Date;

/**
 * Represents data entry of pregnancy observations
 */
public class PregnancyDataEntryTransaction extends DataEntryTransaction {

	private Boolean pregnant;
	private Date expectedDeliveryDate;

	public PregnancyDataEntryTransaction() {}

	public Boolean getPregnant() {
		return pregnant;
	}

	public void setPregnant(Boolean pregnant) {
		this.pregnant = pregnant;
	}

	public Date getExpectedDeliveryDate() {
		return expectedDeliveryDate;
	}

	public void setExpectedDeliveryDate(Date expectedDeliveryDate) {
		this.expectedDeliveryDate = expectedDeliveryDate;
	}
}
