package org.pih.hivmigration.common;

import java.util.List;

/**
 * Represents a cervical cancer screening encounter
 */
public class CervicalCancerEncounter extends Encounter {

	private List<LabTestResult> labResults;

	public CervicalCancerEncounter() {}

	public List<LabTestResult> getLabResults() {
		return labResults;
	}

	public void setLabResults(List<LabTestResult> labResults) {
		this.labResults = labResults;
	}
}
