package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.HivTbStatus;
import org.pih.hivmigration.common.code.Location;
import org.pih.hivmigration.common.code.WhoStagingCriteria;
import org.pih.hivmigration.common.util.ObsName;

import java.util.Date;
import java.util.List;

public class IntakeEncounter extends ClinicalEncounter {

	private String address;
	private String previousDiagnoses;
	private Boolean hospitalizedAtDiagnosis;
	private String differentialDiagnosis;
	private String comments;

	@ObsName("transfer_p") Boolean transferred;
	@ObsName("transfer_in_from") String transferInFrom;
	@ObsName("transfer_out_to")
	Location transferOutTo;

	@ObsName("prophylaxis_not_indicated") Boolean prophylaxisNotIndicated;
	@ObsName("arv_treatment_reason") String arvTreatmentReason;
	@ObsName("category") HivTbStatus hivTbStatus;

	// TODO: Make these into a lab result?
	@ObsName("western_blot_date") Date westernBlotDate;
	@ObsName("western_blot_date_unknown") Boolean westernBlotUnknown;
	@ObsName("western_blot_result") String westernBlotResult;

	private List<Allergy> allergies;
	private List<Contact> contacts;
	private List<Diagnosis> diagnoses;
	private List<PreviousTreatment> previousTreatments;
	private SocioeconomicData socioeconomicData;
	private HivStatusData hivStatusData;
	private List<SystemStatus> systemStatuses;
	private List<WhoStagingCriteria> whoStagingCriteria;

	public IntakeEncounter() {}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getPreviousDiagnoses() {
		return previousDiagnoses;
	}

	public void setPreviousDiagnoses(String previousDiagnoses) {
		this.previousDiagnoses = previousDiagnoses;
	}

	public Boolean getHospitalizedAtDiagnosis() {
		return hospitalizedAtDiagnosis;
	}

	public void setHospitalizedAtDiagnosis(Boolean hospitalizedAtDiagnosis) {
		this.hospitalizedAtDiagnosis = hospitalizedAtDiagnosis;
	}

	public String getDifferentialDiagnosis() {
		return differentialDiagnosis;
	}

	public void setDifferentialDiagnosis(String differentialDiagnosis) {
		this.differentialDiagnosis = differentialDiagnosis;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}

	public Boolean getTransferred() {
		return transferred;
	}

	public void setTransferred(Boolean transferred) {
		this.transferred = transferred;
	}

	public String getTransferInFrom() {
		return transferInFrom;
	}

	public void setTransferInFrom(String transferInFrom) {
		this.transferInFrom = transferInFrom;
	}

	public Location getTransferOutTo() {
		return transferOutTo;
	}

	public void setTransferOutTo(Location transferOutTo) {
		this.transferOutTo = transferOutTo;
	}

	public Boolean getProphylaxisNotIndicated() {
		return prophylaxisNotIndicated;
	}

	public void setProphylaxisNotIndicated(Boolean prophylaxisNotIndicated) {
		this.prophylaxisNotIndicated = prophylaxisNotIndicated;
	}

	public String getArvTreatmentReason() {
		return arvTreatmentReason;
	}

	public void setArvTreatmentReason(String arvTreatmentReason) {
		this.arvTreatmentReason = arvTreatmentReason;
	}

	public HivTbStatus getHivTbStatus() {
		return hivTbStatus;
	}

	public void setHivTbStatus(HivTbStatus hivTbStatus) {
		this.hivTbStatus = hivTbStatus;
	}

	public Date getWesternBlotDate() {
		return westernBlotDate;
	}

	public void setWesternBlotDate(Date westernBlotDate) {
		this.westernBlotDate = westernBlotDate;
	}

	public Boolean getWesternBlotUnknown() {
		return westernBlotUnknown;
	}

	public void setWesternBlotUnknown(Boolean westernBlotUnknown) {
		this.westernBlotUnknown = westernBlotUnknown;
	}

	public String getWesternBlotResult() {
		return westernBlotResult;
	}

	public void setWesternBlotResult(String westernBlotResult) {
		this.westernBlotResult = westernBlotResult;
	}

	public List<Allergy> getAllergies() {
		return allergies;
	}

	public void setAllergies(List<Allergy> allergies) {
		this.allergies = allergies;
	}

	public List<Contact> getContacts() {
		return contacts;
	}

	public void setContacts(List<Contact> contacts) {
		this.contacts = contacts;
	}

	public List<Diagnosis> getDiagnoses() {
		return diagnoses;
	}

	public void setDiagnoses(List<Diagnosis> diagnoses) {
		this.diagnoses = diagnoses;
	}

	public List<PreviousTreatment> getPreviousTreatments() {
		return previousTreatments;
	}

	public void setPreviousTreatments(List<PreviousTreatment> previousTreatments) {
		this.previousTreatments = previousTreatments;
	}

	public SocioeconomicData getSocioeconomicData() {
		return socioeconomicData;
	}

	public void setSocioeconomicData(SocioeconomicData socioeconomicData) {
		this.socioeconomicData = socioeconomicData;
	}

	public HivStatusData getHivStatusData() {
		return hivStatusData;
	}

	public void setHivStatusData(HivStatusData hivStatusData) {
		this.hivStatusData = hivStatusData;
	}

	public List<SystemStatus> getSystemStatuses() {
		return systemStatuses;
	}

	public void setSystemStatuses(List<SystemStatus> systemStatuses) {
		this.systemStatuses = systemStatuses;
	}

	public List<WhoStagingCriteria> getWhoStagingCriteria() {
		return whoStagingCriteria;
	}

	public void setWhoStagingCriteria(List<WhoStagingCriteria> whoStagingCriteria) {
		this.whoStagingCriteria = whoStagingCriteria;
	}
}
