package org.pih.hivmigration.export.query;

import org.pih.hivmigration.common.Address;
import org.pih.hivmigration.common.PamEnrollment;
import org.pih.hivmigration.common.Patient;
import org.pih.hivmigration.common.util.ListMap;
import org.pih.hivmigration.export.DB;

import java.util.Map;

public class PatientQuery {

	/**
	 * The base patient object is essentially mapped to the hiv_demographics table.  It contains the core demographic
	 * data, patient identifiers, and non-encounter-based person attributes, like birthplace and phone number
	 * We are excluding the following data from hiv_demographics
	 *
	 * No data: LAST_NAME2, GPS_LONGITUDE, GPS_LATITUDE, MAIDEN_NAME, COMMENTS,  CIVIL_STATUS, SOCIAL_STATUS, PROFESSION, EDUCATION, PARITY, LIVE_BIRTHS, BIRTH_DATES, EVALUATION_DATE, DATA_COLLECTED_BY
	 * Static / unused data:  CITIZENSHIP (ht), PATIENT_TYPE (pending)
	 *
	 * TODO
	 * Included in HIV program enrollment data:  STARTING_HEALTH_CENTER, HEALTH_CENTER_TRANSFER_DATE, HEALTH_CENTER, TREATMENT_STATUS, TREATMENT_STATUS_DATE, TREATMENT_START_DATE (unsure how to use this)
	 * Not including DOES_NOT_MATCH as not really sure it is needed for migration
	 * Not including ZONE.  This should just go on the intake form if anywhere (it was only ever collected on v1 of the intake form, and not since).  Really part of address.
	 * Figure out what do do with zone and treatment_start_date
	 */
	public static Map<Integer, Patient> getPatients() {
		StringBuilder query = new StringBuilder();
		query.append("select	d.patient_id, d.pih_id, d.nif_id, d.national_id, d.first_name, d.first_name2, d.last_name, ");
		query.append("			d.gender, d.birth_date, d.birth_date_exact_p as birthdateEstimated, d.phone_number, ");
		query.append("			d.birth_place, d.agent as accompagnateur, d.patient_created_by, d.patient_created_date ");
		query.append("from		hiv_demographics d ");
		return DB.mapResult(query, Patient.class);
	}

	public static ListMap<Integer, Address> getAddresses() {
		StringBuilder query = new StringBuilder();
		query.append("select	a.patient_id, a.entry_date, a.address, nvl(l.department, a.department) as department, ");
		query.append("			nvl(l.commune, a.commune) as commune, nvl(l.section_communale, a.section) as section, nvl(l.locality, a.locality) as locality ");
		query.append("from		hiv_addresses a, hiv_locality l ");
		query.append("where		a.locality_id = l.locality_id(+) ");
		query.append("order by	decode(a.type, 'current', 1, 0) desc, a.entry_date desc ");
		return DB.listMapResult(query, Address.class);
	}

	/**
	 * There are a handful of patients that have more than one entry in this table, but upon investigation they are duplicates on all fields, so we can safely do a 1:1 mapping to patient
	 */
	public static Map<Integer, PamEnrollment> getPamEnrollments() {
		StringBuilder query = new StringBuilder();
		query.append("select	patient_id, patient_tx_id as identifier, start_date, end_date ");
		query.append("from		hiv_course_of_tx ");
		return DB.mapResult(query, PamEnrollment.class);
	}
}
