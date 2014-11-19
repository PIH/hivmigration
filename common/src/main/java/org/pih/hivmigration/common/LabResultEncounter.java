package org.pih.hivmigration.common;

import java.util.List;

/**
 * Represents a record of lab results for a patient
 */
public class LabResultEncounter extends Encounter {

	private List<LabTestResult> labResults;
	private String performedBy;
	private String comments;

	public LabResultEncounter() {}

	public List<LabTestResult> getLabResults() {
		return labResults;
	}

	public void setLabResults(List<LabTestResult> labResults) {
		this.labResults = labResults;
	}

	public String getPerformedBy() {
		return performedBy;
	}

	public void setPerformedBy(String performedBy) {
		this.performedBy = performedBy;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}
}
