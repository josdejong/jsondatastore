package com.almende.jsondatastore.entity;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import com.google.appengine.api.datastore.Blob;
import com.google.code.twig.annotation.Id;
import com.google.code.twig.annotation.Index;
import com.google.code.twig.annotation.Type;

public class Attachment {
	@SuppressWarnings("unused")
	@Id private String key = null;
	@SuppressWarnings("unused")
	private String name = null;
	@Index(false) @Type(Blob.class) private byte[] attachment = null; 

	// TODO: implement key
	// TODO: implement name
	
	public void setAttachment(Object attachment) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = new ObjectOutputStream(bos);   
		out.writeObject(attachment);
		this.attachment = bos.toByteArray();
		out.close();
		bos.close();
	}

	public Object getAttachment() throws ClassNotFoundException, IOException {
		ByteArrayInputStream bis = new ByteArrayInputStream(this.attachment);
		ObjectInput in = new ObjectInputStream(bis);
		Object attachment = in.readObject(); 
		bis.close();
		in.close();
		return attachment;
	}
}
