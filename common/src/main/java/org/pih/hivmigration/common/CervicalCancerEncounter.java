package org.pih.hivmigration.common;

import java.util.Date;
import java.util.List;

/**
 * Represents a cervical cancer screening encounter
 */
public class CervicalCancerEncounter extends Encounter {

	private String location;  // TODO: Make this a Location reference
	private Date lastPeriodDate;
	private List<LabTestResult> labResults;

	public CervicalCancerEncounter() {}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public Date getLastPeriodDate() {
		return lastPeriodDate;
	}

	public void setLastPeriodDate(Date lastPeriodDate) {
		this.lastPeriodDate = lastPeriodDate;
	}

	public List<LabTestResult> getLabResults() {
		return labResults;
	}

	public void setLabResults(List<LabTestResult> labResults) {
		this.labResults = labResults;
	}
}
