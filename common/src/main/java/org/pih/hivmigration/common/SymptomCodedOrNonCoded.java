package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.Symptom;

public class SymptomCodedOrNonCoded extends CodedOrNonCoded<Symptom> {

	@Override
	public Class<Symptom> getCodedValueType() {
		return Symptom.class;
	}
}
