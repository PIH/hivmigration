package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.DoubleEntryStatus;
import org.pih.hivmigration.common.code.HivTbStatus;
import org.pih.hivmigration.common.util.ObsName;

import java.util.Date;
import java.util.List;

/**
 * Represents a nutritional evaluation encounter, during the anlap program food study
 */
public class NutritionalEvaluationEncounter extends Encounter {

	private List<LabTestResult> labResults;
	private Double weight;
	private Double height;

	@ObsName("hiv_tb_status") private HivTbStatus hivTbStatus;
	@ObsName("patient_on_arvs") private Boolean onArvs;
	@ObsName("arv_start_date") private Date arvStartDate;
	@ObsName("cd4_below_350") private Boolean cd4Below350;
	@ObsName("patient_interviewed_p") private Boolean patientInterviewed;
	@ObsName("family_member_pam_program_adult_p") private Boolean adultFamilyMemberEnrolledInPam;
	@ObsName("family_member_pam_program_child_p") private Boolean childFamilyMemberEnrolledInPam;
	@ObsName("family_member_pam_program_p") private Boolean otherFamilyMemberEnrolledInPam;
	@ObsName("family_member_in_hiv_program_p") private Boolean otherFamilyMemberInHivProgram;
	@ObsName("ptme_9_to_24m") private Boolean ptmeChildAged9To24m;
	@ObsName("other_nutrition_criteria") String otherNutritionCriteria;
	@ObsName("double_entry_status") DoubleEntryStatus doubleEntryStatus;

	public NutritionalEvaluationEncounter() {}

	public List<LabTestResult> getLabResults() {
		return labResults;
	}

	public void setLabResults(List<LabTestResult> labResults) {
		this.labResults = labResults;
	}

	public Double getWeight() {
		return weight;
	}

	public void setWeight(Double weight) {
		this.weight = weight;
	}

	public Double getHeight() {
		return height;
	}

	public void setHeight(Double height) {
		this.height = height;
	}

	public HivTbStatus getHivTbStatus() {
		return hivTbStatus;
	}

	public void setHivTbStatus(HivTbStatus hivTbStatus) {
		this.hivTbStatus = hivTbStatus;
	}

	public Boolean getOnArvs() {
		return onArvs;
	}

	public void setOnArvs(Boolean onArvs) {
		this.onArvs = onArvs;
	}

	public Date getArvStartDate() {
		return arvStartDate;
	}

	public void setArvStartDate(Date arvStartDate) {
		this.arvStartDate = arvStartDate;
	}

	public Boolean getCd4Below350() {
		return cd4Below350;
	}

	public void setCd4Below350(Boolean cd4Below350) {
		this.cd4Below350 = cd4Below350;
	}

	public Boolean getPatientInterviewed() {
		return patientInterviewed;
	}

	public void setPatientInterviewed(Boolean patientInterviewed) {
		this.patientInterviewed = patientInterviewed;
	}

	public Boolean getAdultFamilyMemberEnrolledInPam() {
		return adultFamilyMemberEnrolledInPam;
	}

	public void setAdultFamilyMemberEnrolledInPam(Boolean adultFamilyMemberEnrolledInPam) {
		this.adultFamilyMemberEnrolledInPam = adultFamilyMemberEnrolledInPam;
	}

	public Boolean getChildFamilyMemberEnrolledInPam() {
		return childFamilyMemberEnrolledInPam;
	}

	public void setChildFamilyMemberEnrolledInPam(Boolean childFamilyMemberEnrolledInPam) {
		this.childFamilyMemberEnrolledInPam = childFamilyMemberEnrolledInPam;
	}

	public Boolean getOtherFamilyMemberEnrolledInPam() {
		return otherFamilyMemberEnrolledInPam;
	}

	public void setOtherFamilyMemberEnrolledInPam(Boolean otherFamilyMemberEnrolledInPam) {
		this.otherFamilyMemberEnrolledInPam = otherFamilyMemberEnrolledInPam;
	}

	public Boolean getOtherFamilyMemberInHivProgram() {
		return otherFamilyMemberInHivProgram;
	}

	public void setOtherFamilyMemberInHivProgram(Boolean otherFamilyMemberInHivProgram) {
		this.otherFamilyMemberInHivProgram = otherFamilyMemberInHivProgram;
	}

	public Boolean getPtmeChildAged9To24m() {
		return ptmeChildAged9To24m;
	}

	public void setPtmeChildAged9To24m(Boolean ptmeChildAged9To24m) {
		this.ptmeChildAged9To24m = ptmeChildAged9To24m;
	}

	public String getOtherNutritionCriteria() {
		return otherNutritionCriteria;
	}

	public void setOtherNutritionCriteria(String otherNutritionCriteria) {
		this.otherNutritionCriteria = otherNutritionCriteria;
	}

	public DoubleEntryStatus getDoubleEntryStatus() {
		return doubleEntryStatus;
	}

	public void setDoubleEntryStatus(DoubleEntryStatus doubleEntryStatus) {
		this.doubleEntryStatus = doubleEntryStatus;
	}
}
