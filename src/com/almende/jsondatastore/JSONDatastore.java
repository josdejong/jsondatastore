package com.almende.jsondatastore;

import static com.google.appengine.api.taskqueue.TaskOptions.Builder.withUrl;

import java.io.IOException;
import java.lang.Exception;
import java.util.UUID;
import java.util.logging.Logger;

import com.almende.jsondatastore.entity.Change;
import com.almende.jsondatastore.entity.Document;
import com.almende.jsondatastore.jackson.JOM;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.code.twig.ObjectDatastore;
import com.google.code.twig.annotation.AnnotationObjectDatastore;

public class JSONDatastore {
	private Logger logger = Logger.getLogger(JSONDatastore.class.getSimpleName());
	private static String SERVLET_URL = "/jsondatastore"; // TODO: do not hardcode url
	
	private String db = null;
	private ObjectMapper mapper = JOM.getInstance();
	
	public JSONDatastore () {}

	public JSONDatastore (String db) {
		setDb(db);
	}

	/**
	 * Set database name
	 * @param db
	 */
	public void setDb(String db) {
		this.db = db;
	}

	/**
	 * Get database name
	 * @return
	 */
	public String getDb() {
		return db;
	}
	
	/**
	 * Get a document by its id
	 * @param id
	 * @param type
	 * @return
	 * @throws Exception
	 */
	public  <T> T get(String id, Class<T> type) throws Exception {
		checkDb();
		ObjectDatastore datastore = new AnnotationObjectDatastore();
		
		String key = Document.createKey(db, id);
		Document doc = datastore.load(Document.class, key);
		if (doc == null) {
			throw new Exception("Document not found");
		}

		return doc.getDoc(type);
	}

	/**
	 * Get a document by its id
	 * @param id
	 * @return
	 * @throws Exception
	 */
	public ObjectNode get(String id) throws Exception {
		checkDb();
		ObjectDatastore datastore = new AnnotationObjectDatastore();
		
		String key = Document.createKey(db, id);
		Document doc = datastore.load(Document.class, key);
		if (doc != null) {
			return doc.getDoc();
		}
		else {
			// TODO: throw an exception instead, like in getDocPojo?
			ObjectNode err = mapper.createObjectNode();
			err.put("error", "not_found");
			err.put("reason", "missing");
			return err;
		}
	}
	
	/**
	 * Get all documents in the current database
	 * @return
	 * @throws Exception
	 */
	public ObjectNode getAllDocs() throws Exception {
		checkDb();
		ObjectDatastore datastore = new AnnotationObjectDatastore();
		ArrayNode rows = mapper.createArrayNode();

		// TODO: change to keys only (optional, keysonly by default)
		QueryResultIterator<Document> results =  
			(QueryResultIterator<Document>) datastore.find().type(Document.class)
			.addFilter("db", FilterOperator.EQUAL, db)
			.now();
		
		while (results.hasNext()) {
			ObjectNode doc = results.next().getDoc();
			rows.add(doc);
		}
		
		int offset = 0; // TODO: implement offset
		ObjectNode result = mapper.createObjectNode();
		result.put("total_rows", rows.size());
		result.put("offset", offset);
		result.put("rows", rows);
		return result;
	}
	
	/**
	 * Create or update a document by its id
	 * @param id
	 * @param doc
	 * @return
	 * @throws Exception
	 */
	public ObjectNode update(String id, ObjectNode doc) 
			throws Exception {
		checkDb();
		ObjectDatastore datastore = new AnnotationObjectDatastore();

		if(id == null) {
			ObjectNode err = mapper.createObjectNode();
			err.put("error", "invalid_id");
			err.put("reason", "no id provided");
			return err;
		}
		if (doc.has("_id")) {
			if (!id.equals(doc.get("_id").asText())) {
				ObjectNode invalidId = mapper.createObjectNode();
				invalidId.put("error", "invalid_id");
				invalidId.put("reason", "id does not match id in document");
				return invalidId;
			}
		}
		doc.put("_id", id);
		
		// retrieve old document
		String key = Document.createKey(db, id);
		Document entity = datastore.load(Document.class, key);
		if (entity != null) {
			// update
			String etagBefore = entity.getEtag();
			
			entity = new Document(db, id, doc);
			datastore.store(entity);
			
			Change change = new Change(db, entity.getId(), etagBefore, entity.getEtag());
			scheduleTask(change);
		}
		else {
			// create
			entity = new Document(db, id, doc);
			datastore.store(entity);
			
			Change change = new Change(db, entity.getId(), null, entity.getEtag());
			scheduleTask(change);
		}
		
		ObjectNode ok = mapper.createObjectNode();
		ok.put("ok", true);
		ok.put("id", id);
		return ok;
	}
	
	/**
	 * Create a new document. An id will be automatically generated. 
	 * @param doc
	 * @return
	 * @throws Exception
	 */
	public ObjectNode create(ObjectNode doc) throws Exception {
		checkDb();
		ObjectDatastore datastore = new AnnotationObjectDatastore();

		String id = null;
		if (doc.has("_id")) {
			ObjectNode err = mapper.createObjectNode();
			err.put("error", "create_failed");
			err.put("reason", "document already has an id");
			return err;
		}

		// generate a random id
		id = UUID.randomUUID().toString().replaceAll("-", "");
		doc.put("_id", id);
		Document entity = new Document(db, id, doc);
		datastore.store(entity);
		
		Change change = new Change(db, entity.getId(), null, entity.getEtag());
		scheduleTask(change);
		
		ObjectNode ok = mapper.createObjectNode();
		ok.put("ok", true);
		ok.put("id", id);
		return ok;
	}
	
	/**
	 * Delete a document by its id
	 * @param id
	 * @return
	 * @throws Exception
	 */
	public ObjectNode delete(String id) throws Exception {
		checkDb();
		ObjectDatastore datastore = new AnnotationObjectDatastore();

		String key = Document.createKey(db, id);
		Document doc = datastore.load(Document.class, key);
		if (doc != null) {
			datastore.delete(doc);

			Change change = new Change(db, doc.getId(), doc.getEtag(), null);
			scheduleTask(change);

			ObjectNode ok = mapper.createObjectNode();
			ok.put("ok", true);
			return ok;
		}
		else {
			// TOOD: throw an exeption instead? or don't throw an error?
			ObjectNode err = mapper.createObjectNode();
			err.put("error", "not_found");
			err.put("reason", "missing");
			return err;
		}
	}

	/**
	 * Get the query results of a view 
	 * @param id    The id of the design document, including the _design prefix,
	 *              for example "_design/myview".
	 * @param view  The name of the view to be retrieved from the design, 
	 *               without the _view prefix.
	 * @return
	 * @throws Exception
	 */
	public ObjectNode getView(String id, String view) throws Exception {
		checkDb();
		View viewObj = new View(db);
		return viewObj.get(id, view);
	}
	
	/**
	 * Get all changes currently in the queue
	 * @return result  an array
	 * @throws Exception
	 */
	public ObjectNode getTaskQueue() throws Exception {
		checkDb();
		ObjectDatastore datastore = new AnnotationObjectDatastore();
		QueryResultIterator<Change> changes = datastore.find().type(Change.class)
				.addFilter("db", FilterOperator.EQUAL, db)
				.now();

		ArrayNode rows = JOM.createArrayNode();
		while (changes.hasNext()) {
			Change change = changes.next();
			rows.add(change.toJSON());			
		}
		
		ObjectNode result = mapper.createObjectNode();
		result.put("total_rows", rows.size());
		result.put("rows", rows);
		return result;
	}
	
	public void scheduleTask(Change change) 
			throws JsonGenerationException, JsonMappingException, IOException {
		ObjectDatastore datastore = new AnnotationObjectDatastore();
		datastore.store(change);
		
		String url = SERVLET_URL + "/" + change.getDb() + "/_queue";
		String body = change.toJSON().toString();
		Queue queue = QueueFactory.getDefaultQueue();
		queue.add(withUrl(url).payload(body));
		logger.info("scheduleTask Change=" + body);
	}
	
	public void executeTask(Change change) throws Exception {
		logger.info("executeTask change=" + change.toJSON().toString());
		View view = new View(db);
		view.process(change);

		ObjectDatastore datastore = new AnnotationObjectDatastore();
		datastore.associate(change);
		datastore.delete(change);
	}
	
	/**
	 * Process a change from the task queue
	 * @return result  An object containing the status message, and the id and
	 *                  etag of the processed update (or null if nothing was
	 *                  in the task queue)
	 * @throws Exception
	 */
	// TODO: delete this
	public ObjectNode processQueue() throws Exception {
		checkDb();
		ObjectDatastore datastore = new AnnotationObjectDatastore();
		
		QueryResultIterator<Change> changes = datastore.find().type(Change.class)
				.addFilter("db", FilterOperator.EQUAL, db)
				.fetchFirst(1)
				.now();
		
		String id = null;
		if (changes.hasNext()) {
			Change change = changes.next();
			id = change.getId();
			
			logger.info("process change " + change.toJSON().toString());
			View view = new View(db);
			view.process(change);
			
			datastore.delete(change);
		}
		
		if (changes.hasNext()) {
			// 	TODO: schedule a new task to process the rest of the changes 
		}
		
		ObjectNode ok = mapper.createObjectNode();
		ok.put("ok", true);
		ok.put("id", id);
		return ok;
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
