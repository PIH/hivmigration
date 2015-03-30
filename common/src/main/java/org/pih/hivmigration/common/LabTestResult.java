package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.CodedValue;
import org.pih.hivmigration.common.code.LabTest;
import org.pih.hivmigration.common.code.LabTestTypeOrBrand;

import java.util.Date;

public class LabTestResult {

	private LabTest labTest;
	private LabTestTypeOrBrand testType;
	private Date testDate;
	private String sampleId;
	private CodedValue valueCoded;
	private Double valueNumeric;
	private Double valuePercent;
	private String valueText;
	private String valueException;

	public LabTestResult() { }

	public LabTestResult(LabTest labTest, Date testDate) {
		this.labTest = labTest;
		this.testDate = testDate;
	}

	public LabTest getLabTest() {
		return labTest;
	}

	public void setLabTest(LabTest labTest) {
		this.labTest = labTest;
	}

	public LabTestTypeOrBrand getTestType() {
		return testType;
	}

	public void setTestType(LabTestTypeOrBrand testType) {
		this.testType = testType;
	}

	public Date getTestDate() {
		return testDate;
	}

	public void setTestDate(Date testDate) {
		this.testDate = testDate;
	}

	public String getSampleId() {
		return sampleId;
	}

	public void setSampleId(String sampleId) {
		this.sampleId = sampleId;
	}

	public CodedValue getValueCoded() {
		return valueCoded;
	}

	public void setValueCoded(CodedValue valueCoded) {
		this.valueCoded = valueCoded;
	}

	public Double getValueNumeric() {
		return valueNumeric;
	}

	public void setValueNumeric(Double valueNumeric) {
		this.valueNumeric = valueNumeric;
	}

	public Double getValuePercent() {
		return valuePercent;
	}

	public void setValuePercent(Double valuePercent) {
		this.valuePercent = valuePercent;
	}

	public String getValueText() {
		return valueText;
	}

	public void setValueText(String valueText) {
		this.valueText = valueText;
	}

	public String getValueException() {
		return valueException;
	}

	public void setValueException(String valueException) {
		this.valueException = valueException;
	}
}
