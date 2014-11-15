package org.pih.hivmigration.common;

import java.util.List;

/**
 * Represents a nutritional evaluation encounter, during the anlap program food study
 */
public class NutritionalEvaluationEncounter extends Encounter {

	private List<LabTestResult> labResults;

	public NutritionalEvaluationEncounter() {}

	public List<LabTestResult> getLabResults() {
		return labResults;
	}

	public void setLabResults(List<LabTestResult> labResults) {
		this.labResults = labResults;
	}
}
