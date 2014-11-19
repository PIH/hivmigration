package org.pih.hivmigration.common;

import java.util.List;

/**
 * Represents a generic encounter with a patient that is not able to be categorized as either an intake or followup
 */
public class PatientContactEncounter extends Encounter {

	private String location;  // TODO: Make this a Location reference
	private List<LabTestResult> labResults;
	private String comments;

	public PatientContactEncounter() {}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public List<LabTestResult> getLabResults() {
		return labResults;
	}

	public void setLabResults(List<LabTestResult> labResults) {
		this.labResults = labResults;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}
}
