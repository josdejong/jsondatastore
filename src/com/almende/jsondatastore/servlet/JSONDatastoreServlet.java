package com.almende.jsondatastore.servlet;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.http.*;

import com.almende.jsondatastore.JSONDatastore;
import com.almende.jsondatastore.entity.Change;
import com.almende.jsondatastore.jackson.JOM;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@SuppressWarnings("serial")
public class JSONDatastoreServlet extends HttpServlet {
	ObjectMapper mapper = JOM.getInstance();

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		// retrieve the database name and document id
		// for example uri="/jsondatastore/db/docid"
		String uri = req.getRequestURI();
		String[] path = uri.split("\\/");
		String db = path.length > 2 ? path[2] : "";
		String id = path.length > 3 ? path[3] : "";
		if (id.equals("_design")) {
			id += "/" + (path.length > 4 ? path[4] : "");
		}
		String view = null;
		if (path.length > 6 && path[5].equals("_view")) {
			view = path[6];
		}
		
		JSONDatastore jds = new JSONDatastore(db);
		String response = "";
		try {
			if (id.equals("_queue")) {
				// return the changes in the queue
				response = jds.getTaskQueue().toString();
			}
			else if (id.equals("_all_docs")) {
				// show all docs
				response = jds.getAllDocs().toString();
			}
			else if (view != null) {
				// show the view results
				response = jds.getView(id, view).toString();
			}
			else {
				// show a document
				response = jds.get(id).toString();
			}
		} catch (Exception e) {
			e.printStackTrace();

			ObjectNode err = mapper.createObjectNode();
			err.put("error", e.getClass().getSimpleName());
			err.put("reason", e.getMessage());
			response = err.toString();
		}
		
		resp.setContentType("application/json");
		resp.getWriter().println(response);
	}

	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		// retrieve the request body
		String body = streamToString(req.getInputStream()).trim();

		// retrieve the database name and document id
		// for example uri="/jsondatastore/db/docid"
		String uri = req.getRequestURI();
		String[] path = uri.split("\\/");
		String db = path.length > 2 ? path[2] : "";
		String id = path.length > 3 ? path[3] : "";
		// TODO: throw error when id is provided

		JSONDatastore jds = new JSONDatastore(db);
		String response = "";
		try {
			if (id.equals("_queue")) {
				// execute queue
				Change change = mapper.readValue(body, Change.class);
				jds.executeTask(change);
				response = change.toJSON().toString();
			}
			else if (!id.isEmpty()) {
				ObjectNode err = mapper.createObjectNode();
				err.put("error", "invalid_id");
				err.put("reason", "id not allowed");
				response = err.toString();
			}
			else {
				ObjectNode doc = mapper.readValue(body, ObjectNode.class);
				response = jds.create(doc).toString();
			}
			
		} catch (Exception e) {
			e.printStackTrace();

			ObjectNode err = mapper.createObjectNode();
			err.put("error", e.getClass().getSimpleName());
			err.put("reason", e.getMessage());
			response = err.toString();
		}

		resp.setContentType("application/json");
		resp.getWriter().println(response);		
	}

	@Override
	public void doPut(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		// retrieve the request body
		String body = streamToString(req.getInputStream()).trim();

		// retrieve the database name and document id
		// for example uri="/jsondatastore/db/docid"
		String uri = req.getRequestURI();
		String[] path = uri.split("\\/");
		String db = path.length > 2 ? path[2] : "";
		String id = path.length > 3 ? path[3] : null;
		if (id.equals("_design")) {
			id += "/" + (path.length > 4 ? path[4] : "");
		}

		JSONDatastore jds = new JSONDatastore(db);
		String response = "";
		try {
			ObjectNode doc = mapper.readValue(body, ObjectNode.class);
			
			response = jds.update(id, doc).toString();
		} catch (Exception e) {
			e.printStackTrace();
			
			ObjectNode err = mapper.createObjectNode();
			err.put("error", e.getClass().getSimpleName());
			err.put("reason", e.getMessage());
			response = err.toString();
		}

		resp.setContentType("application/json");
		resp.getWriter().println(response);		
	}

	@Override
	public void doDelete(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		// retrieve the database name and document id
		// for example uri="/jsondatastore/db/docid"
		String uri = req.getRequestURI();
		String[] path = uri.split("\\/");
		String db = path.length > 2 ? path[2] : "";
		String id = path.length > 3 ? path[3] : "";
		if (id.equals("_design")) {
			id += "/" + (path.length > 4 ? path[4] : "");
		}
		
		JSONDatastore jds = new JSONDatastore(db);
		String response = "";
		try {
			response = jds.delete(id).toString();
		} catch (Exception e) {
			e.printStackTrace();

			ObjectNode err = mapper.createObjectNode();
			err.put("error", e.getClass().getSimpleName());
			err.put("reason", e.getMessage());
			response = err.toString();
		}
		
		resp.setContentType("application/json");
		resp.getWriter().println(response);
	}
	
	private static String streamToString(InputStream in) throws IOException {
		StringBuffer out = new StringBuffer();
		byte[] b = new byte[4096];
		for (int n; (n = in.read(b)) != -1;) {
			out.append(new String(b, 0, n));
		}
		return out.toString();
	}
}
