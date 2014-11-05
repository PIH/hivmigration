package org.pih.hivmigration.common;

import java.util.Date;

public class PreviousTreatment {

	private String treatmentCoded;
	private String treatmentNonCoded;
	private Date startDate;
	private Date endDate;
	private String outcome;

	public PreviousTreatment() {}

	public String getOutcome() {
		return outcome;
	}

	public void setOutcome(String outcome) {
		this.outcome = outcome;
	}

	public String getTreatmentCoded() {
		return treatmentCoded;
	}

	public void setTreatmentCoded(String treatmentCoded) {
		this.treatmentCoded = treatmentCoded;
	}

	public String getTreatmentNonCoded() {
		return treatmentNonCoded;
	}

	public void setTreatmentNonCoded(String treatmentNonCoded) {
		this.treatmentNonCoded = treatmentNonCoded;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
}
