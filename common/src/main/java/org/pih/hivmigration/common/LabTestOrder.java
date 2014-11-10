package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.LabTest;

public class LabTestOrder {

	private LabTest testCoded;
	private String testNonCoded;

	public LabTestOrder() {}

	public LabTest getTestCoded() {
		return testCoded;
	}

	public void setTestCoded(LabTest testCoded) {
		this.testCoded = testCoded;
	}

	public String getTestNonCoded() {
		return testNonCoded;
	}

	public void setTestNonCoded(String testNonCoded) {
		this.testNonCoded = testNonCoded;
	}
}
