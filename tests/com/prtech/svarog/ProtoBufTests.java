package com.prtech.svarog;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.gson.JsonObject;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;

public class ProtoBufTests {

	
	
	@Test
	public void protoTest() {
		DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
		schemaBuilder.setName("PersonSchemaDynamic.proto");

		MessageDefinition msgDef = MessageDefinition.newBuilder("Person") // message
																			// Person
				.addField("required", "int32", "id", 1) // required int32 id = 1
				.addField("required", "string", "name", 2) // required string
															// name = 2
				.addField("optional", "string", "email", 3) // optional string.
															// email = 3
				.build();

		MessageDefinition arrDef = MessageDefinition.newBuilder("PersonArray").addMessageDefinition(msgDef)
				.addField("repeated", "Person", "persons", 1).build();

		
		schemaBuilder.addMessageDefinition(msgDef);
		schemaBuilder.addMessageDefinition(arrDef);
		DynamicSchema schema = null;
		try {
			schema = schemaBuilder.build();

			// Create dynamic message from schema
			DynamicMessage.Builder msgBuilder = schema.newMessageBuilder("Person");
			Descriptor msgDesc = msgBuilder.getDescriptorForType();
			

			DynamicMessage msg = msgBuilder.setField(msgDesc.findFieldByName("id"), 1)
					.setField(msgDesc.findFieldByName("name"), "Kocho Nicha")
					.setField(msgDesc.findFieldByName("email"), "kocho@vlav.com").build();

			DynamicMessage msg1 = msgBuilder.setField(msgDesc.findFieldByName("id"), 2)
					.setField(msgDesc.findFieldByName("name"), "Kosta Novakovik")
					.setField(msgDesc.findFieldByName("email"), "kosta@novakovik.com").build();

			
			DynamicMessage.Builder arrBuilder = schema.newMessageBuilder("PersonArray");
			Descriptor arrDesc = arrBuilder.getDescriptorForType();

			String s=arrDesc.getFile().toString();
			String s2=arrDesc.getFile().toProto().toString();
			System.out.println(s2);
			FileDescriptor fd= arrDesc.getFile();
			
			FieldDescriptor f=arrDesc.findFieldByName("persons");
			ArrayList<DynamicMessage> l = new ArrayList<DynamicMessage>();
			l.add(msg);
			l.add(msg1);
			DynamicMessage msgarr = arrBuilder.setField(f, l).build();

			byte[] pbbarr = msgarr.toByteArray();
			byte[] pbb = msg.toByteArray();

			DynamicMessage msg2 = DynamicMessage.parseFrom(arrDesc, pbbarr);
			List<DynamicMessage> l2=(List<DynamicMessage>)msg2.getField(arrDesc.findFieldByName("persons"));
			
			for(DynamicMessage m7:l2)
				System.out.println(m7.toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
