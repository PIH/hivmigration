package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.CodedValue;

public abstract class CodedOrNonCoded<T extends CodedValue> {

	private T codedValue;
	private String nonCodedValue;

	public CodedOrNonCoded() {}

	public abstract Class<T> getCodedValueType();

	public T getCodedValue() {
		return codedValue;
	}

	public void setCodedValue(T codedValue) {
		this.codedValue = codedValue;
	}

	public String getNonCodedValue() {
		return nonCodedValue;
	}

	public void setNonCodedValue(String nonCodedValue) {
		this.nonCodedValue = nonCodedValue;
	}
}
