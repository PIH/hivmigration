package org.pih.hivmigration.common.code;

public enum HivStatus implements CodedValue {

	POSITIVE("t"),
	NEGATIVE("f"),
	UNKNOWN("?");

	// ***** BOILERPLATE *****
	private String value;
	HivStatus(String value) { this.value = value; }
	public String getValue() { return value; }
}
