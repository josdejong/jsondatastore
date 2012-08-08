package com.almende.jsondatastore;

import java.util.Date;
import java.util.Iterator;
import java.util.logging.Logger;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;

import com.almende.jsondatastore.entity.Change;
import com.almende.jsondatastore.entity.Document;
import com.almende.jsondatastore.entity.Output;
import com.almende.jsondatastore.jackson.JOM;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.code.twig.ObjectDatastore;
import com.google.code.twig.annotation.AnnotationObjectDatastore;

public class View {
	private Logger logger = Logger.getLogger(View.class.getSimpleName());
	private String db = null; 
	
	public View () {}
	
	public View (String db) {
		setDb(db);
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getDb() {
		return db;
	}

	public ObjectNode get(String id, String view) 
			throws Exception { 
		return get(id, view, null, null);
	}

	public ObjectNode get(String id, String view, 
			String startkey, String endkey) throws Exception {
		checkDb();
		ObjectDatastore datastore = new AnnotationObjectDatastore();
		
		// retrieve the design document
		String key = Document.createKey(db, id);
		Document design = datastore.load(Document.class, key);
		if (design == null) {
			throw new Exception("Document not found");
		}
		
		// iterate over all view outputs
		QueryResultIterator<Output> outputs = 
			(QueryResultIterator<Output>) datastore.find().type(Output.class)
			.addFilter("db", FilterOperator.EQUAL, db)
			.addFilter("designId", FilterOperator.EQUAL, id)
			.addFilter("view", FilterOperator.EQUAL, view)
			// TODO: apply startKey and endKey
			.now();
		ArrayNode rows = JOM.createArrayNode();
		while (outputs.hasNext()) {
			// TODO: the getResult of the output is quite inefficient. 
			//       better stream the output directly as string?
			Output output = outputs.next();
			rows.add(output.getResult());
		}

		// create the response
		ObjectNode results = JOM.createObjectNode();
		results.put("total_rows", rows.size());
		results.put("rows", rows);
		return results;
	}
	
	/**
	 * Process a change
	 * @param change
	 * @throws Exception 
	 */
	public void process(Change change) throws Exception {
		checkDb();
		ObjectDatastore datastore = new AnnotationObjectDatastore();
		
		if (change.getEtagAfter() != null) {  // CREATE or UPDATE
			// update outputs
			String key = Document.createKey(db, change.getId());
			Document doc = datastore.load(Document.class, key);
			if (doc != null) {
				update(datastore, doc);
				
				// TODO: test if this works. It is dangerous as the indexes may not yet be updated completely 
				// verify if the concerning document hasn't changed during the process
				Document checkDoc = datastore.load(Document.class, key);
				if (doc.getEtag() != checkDoc.getEtag()) {
					// that is a pity, we can directly destroy our work again,
					// the document has ben changed while updating
					delete(datastore, doc.getId(), doc.getEtag());
				}
			}
		}
		
		if (change.getEtagBefore() != null) {  // DELETE or UPDATE
			// remove old outputs
			delete(datastore, change.getId(), change.getEtagBefore());
		}
	}
	
	/**
	 * Update the view outputs for given document
	 * @param datastore
	 * @param doc
	 * @throws Exception
	 */
	public void update(ObjectDatastore datastore, Document doc) 
			throws Exception {
		QueryResultIterator<Document> designs = datastore.find()
			.type(Document.class)
			.addFilter("db", FilterOperator.EQUAL, db)
			.addFilter("design", FilterOperator.EQUAL, true)
			.now();
		
		long start = new Date().getTime();
		int count = 0;
		while (designs.hasNext()) {
			Document design = designs.next();
			//logger.info("update designId=" + design.getId() + ", docId=" + doc.getId());
			execute(design, doc);
			count++;
		}
		long end = new Date().getTime();
		logger.info("updated " + count + " designs for docId=" + doc.getId() + 
				" in " + (end - start) + "ms");
		
		if (doc.isDesign()) {
			updateDesign(datastore, doc);
		}
	}

	/**
	 * Process the view outputs for given design document,
	 * this will iterate the design for all documents in the database 
	 * @param datastore
	 * @param design
	 * @throws Exception
	 */
	private void updateDesign(ObjectDatastore datastore, Document design) 
			throws Exception {

		QueryResultIterator<Document> docs = datastore.find()
			.type(Document.class)
			.addFilter("db", FilterOperator.EQUAL, db)
			.now();
		
		long start = new Date().getTime();
		int count = 0;
		while (docs.hasNext()) {
			Document doc = docs.next();
			// logger.info("update designId=" + design.getId() +	", docId=" + doc.getId());
			execute(design, doc);
			count ++;
		}
		long end = new Date().getTime();
		logger.info("updated " + count + " designs for designId=" + design.getId() + 
				" in " + (end - start) + "ms");

	}

	/**
	 * Delete view outputs with given etag
	 * @param datastore
	 * @param id
	 * @param etag
	 */
	private void delete (ObjectDatastore datastore, String id, String etag) {
		QueryResultIterator<Output> outputs = datastore.find()
				.type(Output.class)
				.addFilter("docEtag", FilterOperator.EQUAL, etag)
				.now();
		
		long start = new Date().getTime();
		int count = 0;
		while (outputs.hasNext()) {
			Output output = outputs.next();
			// logger.info("delete designId=" + output.getDesignId() + ", docId=" + output.getDocId());
			datastore.delete(output);
			count++;
		}
		long end = new Date().getTime();
		logger.info("deleted " + count + " outputs from docId=" + id + 
				" in " + (end - start) + "ms");
		
		if (id.startsWith("_design/")) {
			deleteDesign(datastore, id, etag);
		}
	}

	/**
	 * Delete view outputs with given etag
	 * @param datastore
	 * @param id
	 * @param etag
	 */
	private void deleteDesign (ObjectDatastore datastore, String id, String etag) {
		QueryResultIterator<Output> outputs = datastore.find()
				.type(Output.class)
				.addFilter("designEtag", FilterOperator.EQUAL, etag)
				.now();

		long start = new Date().getTime();
		int count = 0;
		while (outputs.hasNext()) {
			Output output = outputs.next();
			//logger.info("delete designId=" + output.getDesignId() + ", docId=" + output.getDocId());
			datastore.delete(output);
			count++;
		}
		long end = new Date().getTime();
		logger.info("deleted " + count + " outputs from designId=" + id + 
				" in " + (end - start) + "ms");
	}
	
	/**
	 * Execute the views from a design document on a document 
	 * @param design
	 * @param doc
	 * @throws Exception 
	 */
	private void execute(Document design, Document doc) throws Exception {
		// TODO: the method execute is incredibly inefficient. improve the performance
		checkDb();
		ObjectDatastore datastore = new AnnotationObjectDatastore();
		
		// create a Script engine
		Context cx = Context.enter();
		Scriptable scope = cx.initStandardObjects();

		try {
			ObjectNode jsonDesign = design.getDoc();
			// create the views with maps
			if (!jsonDesign.has("views")) {
				throw new Exception("Field \"views\" missing in design document");
			}

			String js = 
				"var id = undefined;" +   // current id
				"var view = undefined;" + // current view
				"var maps = {};" +
				"var output = {};";
	
			// read all map functions
			ObjectNode views = (ObjectNode) jsonDesign.get("views");
			Iterator<String> keys = views.fieldNames();
			while (keys.hasNext()) {
				String viewName = keys.next();
				ObjectNode view = (ObjectNode) views.get(viewName);
				if (!view.has("map")) {
					throw new Exception("Field \"views." + view + 
							".map\" missing in design document");
				}
				String map = view.get("map").asText();
				js += "maps." + viewName + " = " + map + ";";
				js += "output." + viewName + " = [];";
			}
			
			// create emit and query functions
			js +=
				"function emit(key, value) {" +
					"output[view].push({'id': id, 'key': key, 'value': value});" + 
				"};" +
				"function queryDoc(doc) {" + 
					"for (view in maps) {" + 
						"if (maps.hasOwnProperty(view)) {" +
							"var map = maps[view]; " +  
							"if (map) {" +
								"map(doc);" +
							"}" + 
						"}" +
					"}" +
				"}";
			
			cx.evaluateString(scope, js, "<cmd>", 1, null);
			
			// process the document
			// TODO: can I access the document directly from Java?
			String str = "var doc = " + doc.getDocString() + ";" 
				+ "id = doc._id;"
				+ "queryDoc(doc);";
			cx.evaluateString(scope, str, "<cmd>", 1, null);

			// retrieve the emitted output from javascript and store the results
			// in the datastore
			ObjectMapper mapper = JOM.getInstance();
			ObjectNode allOutput = mapper.convertValue(scope.get("output", scope), 
					ObjectNode.class);
			Iterator<String> viewNames = allOutput.fieldNames();
			while (viewNames.hasNext()) {
				String viewName = viewNames.next();
				ArrayNode viewOutput = (ArrayNode) allOutput.get(viewName);
				for (int i = 0; i < viewOutput.size(); i++) {
					ObjectNode jsonOutput = (ObjectNode) viewOutput.get(i);
					Output output = new Output(db, design.getId(), design.getEtag(),
							viewName, doc.getId(), doc.getEtag(), i, jsonOutput);
					
					// store new results
					datastore.store(output);
				}
			}
		}
		finally {
			Context.exit();
		}
		
	}
	
	/**
	 * Check whether there is a legal database name set
	 * @throws Exception
	 */
	private void checkDb() throws Exception {
		if (this.db == null) {
			throw new Exception("db not initialized");
		}
		// TODO: check for illegal characters
	}
}
