/**
 * @file Document.java
 * 
 * @brief 
 * The entity Document stores a single JSON Object as a string in the 
 * google datastore.
 *
 * @license
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy 
 * of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * Copyright © 2010-2011 Almende B.V.
 *
 * @author 	Jos de Jong, <jos@almende.org>
 * @date	  2012-03-30
 */
package com.almende.jsondatastore.entity;

import java.io.IOException;
import java.util.UUID;

import com.almende.jsondatastore.jackson.JOM;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.code.twig.annotation.Id;
import com.google.code.twig.annotation.Index;

public class Document {
	@SuppressWarnings("unused") private @Id String key = null; // key is a composite key "db/id"
	private String etag = UUID.randomUUID().toString();
	private String db = null;
	@Index(false) private String id = null;
	@Index(false) private String doc = null;
	private boolean design = false;  // true if the document is a design document 
	
	protected Document () {}
	
	public Document (String db, String id, String doc) 
			throws JsonParseException, JsonMappingException, IOException {
		setDb(db);
		setId(id);
		setKey(db, id);		
		setDocString(doc);
	}

	public Document (String db, String id, ObjectNode doc) 
			throws JsonGenerationException, JsonMappingException, IOException {
		setDb(db);
		setId(id);
		setKey(db, id);
		setDoc(doc);
	}
	
	public void setId(String id) {
		this.id = id;
		this.design = (id != null) ? id.startsWith("_design/") : false;
		setKey(db, id);
	}

	public void setKey(String db, String id) {
		key = createKey(db, id);
	}
	
	public String getId() {
		return id;
	}

	public String getEtag() {
		return etag;
	}

	public boolean isDesign() {
		return design;
	}
	
	public void setDb(String db) {
		this.db = db;
		this.key = createKey(db, id);
	}

	public String getDb() {
		return db;
	}

	public static String createKey(String db, String id) {
		return (db != null ? db : "") + "/" + (id != null ? id : "");
	}

	public void setDocString(String doc) 
			throws JsonParseException, JsonMappingException, IOException {
		// verify if the doc is valid json
		JOM.getInstance().readValue(doc, ObjectNode.class);
		
		this.doc = doc;
	}
	
	public String getDocString() {
		return doc;
	}
	
	public void setDoc(ObjectNode doc) 
			throws JsonGenerationException, JsonMappingException, IOException {
		this.doc = JOM.getInstance().writeValueAsString(doc);
	}

	public <T> void setDoc (T doc) 
			throws JsonGenerationException, JsonMappingException, IOException {
		this.doc = JOM.getInstance().writeValueAsString(doc);
	}

	public ObjectNode getDoc() 
			throws JsonParseException, JsonMappingException, IOException {
		return JOM.getInstance().readValue(doc, ObjectNode.class);
	}

	public <T> T getDoc(Class<T> type) 
			throws JsonParseException, JsonMappingException, IOException {
		return JOM.getInstance().readValue(doc, type);
	}

}	