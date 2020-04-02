/*******************************************************************************
 *   Copyright (c) 2013, 2019 Perun Technologii DOOEL Skopje.
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Apache License
 *   Version 2.0 or the Svarog License Agreement (the "License");
 *   You may not use this file except in compliance with the License. 
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See LICENSE file in the project root for the specific language governing 
 *   permissions and limitations under the License.
 *  
 *******************************************************************************/
 
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

public class ProtoBufTest {

	
	
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

			DynamicMessage receivedMsg = DynamicMessage.parseFrom(arrDesc, pbbarr);
			List<DynamicMessage> decodedList=(List<DynamicMessage>)receivedMsg.getField(arrDesc.findFieldByName("persons"));
			
			for(DynamicMessage decodedMsg:decodedList)
				System.out.println(decodedMsg.toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
