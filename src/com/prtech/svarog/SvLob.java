package com.prtech.svarog;

import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SvLob implements AutoCloseable {

	private final Connection connection;
	private List<Blob> blobs;
	private List<Clob> clobs;
	private List<StringReader> readers;

	List<Blob> getBlobs() {
		if (blobs == null)
			blobs = new ArrayList<>();
		return blobs;
	}

	List<Clob> getClobs() {
		if (clobs == null)
			clobs = new ArrayList<>();
		return clobs;
	}

	List<StringReader> getReaders() {
		if (readers == null)
			readers = new ArrayList<>();
		return readers;
	}

	public SvLob(Connection connection) {
		this.connection = connection;
	}

	public final Blob blob(byte[] bytes) throws SQLException {
		Blob blob;

		// You may write more robust dialect
		// detection here
		blob = connection.createBlob();
		blob.setBytes(1, bytes);
		blobs.add(blob);
		return blob;
	}

	public final Clob clob(String string) throws SQLException {
		Clob clob;
		clob = connection.createClob();
		clob.setString(1, string);
		getClobs().add(clob);
		return clob;
	}

	@Override
	public final void close() throws Exception {
		if (blobs != null)
			for (Blob b : blobs)
				b.free();
		if (clobs != null)
			for (Clob c : clobs)
				c.free();
		if (readers != null)
			for (StringReader s : readers)
				s.close();
	}

	public StringReader stringReader(String string) {
		// TODO Auto-generated method stub
		java.io.StringReader reader = new java.io.StringReader(string);
		getReaders().add(reader);
		return reader;
	}

}
