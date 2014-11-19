package org.pih.hivmigration.common;

/**
 * Represents a note about the patient or a particular encounter
 */
public class Note extends Encounter {

	private String noteTitle;
	private String comments;

	public Note() {}

	public String getNoteTitle() {
		return noteTitle;
	}

	public void setNoteTitle(String noteTitle) {
		this.noteTitle = noteTitle;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}
}
