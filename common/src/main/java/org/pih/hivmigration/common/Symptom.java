package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.SymptomConcept;

public class Symptom extends CodedOrNonCoded<SymptomConcept> {

	@Override
	public Class<SymptomConcept> getCodedValueType() {
		return SymptomConcept.class;
	}
}
