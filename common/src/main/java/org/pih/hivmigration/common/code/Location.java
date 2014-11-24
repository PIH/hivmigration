package org.pih.hivmigration.common.code;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Remove any locations that actually aren't used
 */
public enum Location implements CodedValue {

	BAPTISTE(35),
	BELLADERES(5),
	BOUCAN_CARRE(3),
	CANGE(4),
	CERCA_CAVAJAL(31),
	CERCA_LA_SOURCE(21),
	DUFAILLY(39),
	HINCHE(6),
	LASCAHOBAS(8),
	MAISSADE(36),
	MIREBALAIS(33),
	OTHER_NON_ZL(42),
	PETITE_RIVIERE(37),
	POZ(40),
	ST_MARC_HSN(82),
	ST_MARC_SSPE(22),
	SAVANETTE(34),
	THOMONDE(7),
	THOMASSIQUE(32),
	TILORY(38),
	VERRETTES(63);

	private Integer siteId;

	Location(Integer siteId) {
		this.siteId = siteId;
	}

	@Override
	public List<String> getValues() {
		List<String> l = new ArrayList<String>();
		l.add(name());
		l.add(siteId.toString());
		return l;
	}
}
