package com.almende.jsondatastore.entity;

import java.io.IOException;

import com.almende.jsondatastore.jackson.JOM;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.code.twig.annotation.Id;
import com.google.code.twig.annotation.Index;

public class Output {
	@Id String key = null;
	private String db = null;
	private String designId = null;
	private String designEtag = null;
	private String view = null;
	private String docId = null;
	private String docEtag = null;
	private Integer emitIndex = null;
	@Index(false) private String result = null;  // JSONObject containing id, key, value 
	// TODO: optimize this class, add @Index(false) as much as possible, remove redundant fields
	
	protected Output () {}
	
	public Output (String db, String designId, String designEtag, String view, 
			String docId, String docEtag, Integer emitIndex, JsonNode result) {
		setKey(db, designEtag, view, docEtag, emitIndex);
		setDb(db);
		setDesignId(designId);
		setDesignEtag(designEtag);
		setView(view);
		setDocId(docId);
		setDocEtag(docEtag);
		setEmitIndex(emitIndex);
		setResult(result);
	}

	private void setKey(String db, String designEtag, String view, 
			String docEtag, Integer emitIndex) {
		key = db + "/" + designEtag + "/" + view + "/" + docEtag + "/" + emitIndex;
	}
	
	public void setResult(JsonNode result) {
		this.result = result.toString();
	}

	public JsonNode getResult() throws JsonProcessingException, IOException {
		ObjectMapper mapper = JOM.getInstance();
		return mapper.readTree(result);
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getDb() {
		return db;
	}

	public void setDesignId(String designId) {
		this.designId = designId;
	}

	public String getDesignId() {
		return designId;
	}

	public void setDesignEtag(String designEtag) {
		this.designEtag = designEtag;
	}

	public String getDesignEtag() {
		return designEtag;
	}

	public void setView(String view) {
		this.view = view;
	}

	public String getView() {
		return view;
	}

	public void setDocId(String docId) {
		this.docId = docId;
	}

	public String getDocId() {
		return docId;
	}

	public void setDocEtag(String docEtag) {
		this.docEtag = docEtag;
	}

	public String getDocEtag() {
		return docEtag;
	}

	public void setEmitIndex(Integer emitIndex) {
		this.emitIndex = emitIndex;
	}

	public Integer getEmitIndex() {
		return emitIndex;
	}
}
