package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the severity of a rash etc
 */
public enum SideEffect implements CodedValue {

	ANEMIA,
	CNS_SYMPTOMS,
	DIARRHEA,
	HEPATITIS,
	JAUNDICE,
	LACTIC_ACID,
	NAUSEA,
	NEUROPATHY,
	OTHER,
	RASH;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name().toLowerCase());
	}
}
