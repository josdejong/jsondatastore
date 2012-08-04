package com.almende.jsondatastore.entity;

import com.almende.jsondatastore.jackson.JOM;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.code.twig.annotation.Id;
import com.google.code.twig.annotation.Index;

public class Change {
	@Id Long key = null;
	private String db = null;                        // the database
	@Index(false) private String id = null;         // id of the document
	@Index(false) private String etagBefore = null; // etag before change
	@Index(false) private String etagAfter = null;  // etag after change
		
	public Change() {}
	
	public Change(String db, String id, String etagBefore, String etagAfter) {
		this.setDb(db);
		this.setId(id);
		this.setEtagBefore(etagBefore);
		this.setEtagAfter(etagAfter);
	}

	public void setKey(Long key) {
		this.key = key;
	}

	public Long getKey() {
		return key;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setEtagBefore(String etagBefore) {
		this.etagBefore = etagBefore;
	}

	public String getEtagBefore() {
		return etagBefore;
	}
	
	public void setEtagAfter(String etagAfter) {
		this.etagAfter = etagAfter;
	}

	public String getEtagAfter() {
		return etagAfter;
	}
	
	public void setDb(String db) {
		this.db = db;
	}

	public String getDb() {
		return db;
	}
	
	public ObjectNode toJSON() {
		ObjectNode json = JOM.createObjectNode();
		json.put("key", key);
		json.put("db", db);
		json.put("id", id);
		json.put("etagBefore", etagBefore);
		json.put("etagAfter", etagAfter);
		return json;
	}
}
