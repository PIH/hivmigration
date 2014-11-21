package org.pih.hivmigration.common;

import java.util.Date;

/**
 * Represents data entry of pregnancy observations
 */
public class PregnancyDataEntryTransaction extends DataEntryTransaction {

	private Boolean pregnant;
	private Date expectedDeliveryDate;

	@ObsName("arvs_for_ptme") Boolean arvsForPtme;
	@ObsName("arv_for_ptme_start_date") Date arvForPtmeStartDate;
	@ObsName("arv_for_ptme_stop_date") Date arvForPtmeStopDate;

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

	public Boolean getArvsForPtme() {
		return arvsForPtme;
	}

	public void setArvsForPtme(Boolean arvsForPtme) {
		this.arvsForPtme = arvsForPtme;
	}

	public Date getArvForPtmeStartDate() {
		return arvForPtmeStartDate;
	}

	public void setArvForPtmeStartDate(Date arvForPtmeStartDate) {
		this.arvForPtmeStartDate = arvForPtmeStartDate;
	}

	public Date getArvForPtmeStopDate() {
		return arvForPtmeStopDate;
	}

	public void setArvForPtmeStopDate(Date arvForPtmeStopDate) {
		this.arvForPtmeStopDate = arvForPtmeStopDate;
	}
}
