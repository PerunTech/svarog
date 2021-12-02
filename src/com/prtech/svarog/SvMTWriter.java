package com.prtech.svarog;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

public class SvMTWriter implements java.lang.AutoCloseable {
	private static final Logger log4j = SvConf.getLogger(SvMTWriter.class);

	private final List<SvWriterThread> writerThreads;
	private final List<Thread> threads;
	private final AtomicBoolean isMTRunning = new AtomicBoolean(false);

	private class SvWriterThread implements Runnable {
		private final AtomicBoolean isRunning = new AtomicBoolean(false);
		private final AtomicBoolean isBusy = new AtomicBoolean(false);

		SvWriter writer = null;
		DbDataArray currentItems;
		SvException raisedException = null;
		Object notifier;
		boolean isBatch = false;
		int offset = 0;

		// the writer instance to be used and a notifier object to be used for
		// notification when done
		SvWriterThread(SvWriter svw, Object notifier) {
			this.writer = svw;
			this.notifier = notifier;
		}

		public void clearException() {
			raisedException = null;
		}

		public void saveObject(DbDataArray newItems, boolean isBatch) throws SvException {
			if (isBusy.get())
				throw (new SvException("system.err.thread_busy", this.writer.instanceUser));
			else {
				if (raisedException != null)
					throw (new SvException("system.err.previous_exception", this.writer.instanceUser, raisedException));

				this.currentItems = newItems;
				this.isBatch = isBatch;
				isBusy.set(true);
				synchronized (this.isRunning) {
					this.isRunning.notifyAll();
				}
			}
		}

		@Override
		public void run() {
			this.isRunning.set(true);
			while (this.isRunning.get()) {
				try {
					if (currentItems == null)
						synchronized (isRunning) {
							this.isRunning.wait(10);
						}

					if (currentItems != null)
						this.writer.saveObject(currentItems, isBatch);

				} catch (SvException | InterruptedException e) {
					if (e instanceof SvException)
						this.raisedException = (SvException) e;
					else
						this.raisedException = new SvException("system.err.thread_interrupted",
								this.writer.instanceUser, e);
				} finally {
					this.isBusy.set(false);
					this.currentItems = null;
				}
				synchronized (this.notifier) {
					this.notifier.notifyAll();
				}
			}
		}

	}

	/**
	 * Constructor of MultiThreaded SvWriter. It will accept a list of writers and
	 * orchestrate multhreaded database writing.
	 * 
	 * @param writers List of SvWriters. Each of the writers must be standalone
	 *                (i.e. can't be shared)
	 * @throws SvException If any of the writers in the list has a valid parent
	 *                     core, "system.err.sv_core_is_shared" exception will be
	 *                     thrown
	 */
	public SvMTWriter(List<SvWriter> writers) throws SvException {
		writerThreads = new ArrayList<SvWriterThread>(writers.size());
		threads = new ArrayList<Thread>(writers.size());
		for (SvWriter svw : writers) {
			if (svw.getParentSvCore() != null)
				throw (new SvException("system.err.sv_core_is_shared", svw.instanceUser));
			svw.setAutoCommit(false);
			SvWriterThread st = new SvWriterThread(svw, this.isMTRunning);
			writerThreads.add(st);
		}

	}

	public void start() {
		if (threads.isEmpty() && isMTRunning.compareAndSet(false, true)) {
			for (SvWriterThread swt : writerThreads) {
				Thread t = new Thread(swt);
				threads.add(t);
				t.start();
			}
		}

	}

	SvWriterThread getFirstAvailable() {
		for (SvWriterThread swt : writerThreads) {
			if (swt.isBusy.get())
				continue;
			else
				return swt;
		}
		return null;
	}

	public boolean allDone() {
		for (SvWriterThread swt : writerThreads) {
			if (swt.isBusy.get())
				return false;
		}
		return true;
	}

	public void commit() throws SvException {
		if (allDone()) {
			for (SvWriterThread swt : writerThreads) {
				swt.writer.dbCommit();
				swt.clearException();
			}
		} else
			throw (new SvException("system.err.thread_busy", svCONST.systemUser));
	}

	public void rollback() throws SvException {
		if (allDone()) {
			for (SvWriterThread swt : writerThreads) {
				if (swt.writer != null)
					swt.writer.dbRollback();
				swt.clearException();
			}
		} else
			throw (new SvException("system.err.thread_busy", svCONST.systemUser));
	}

	public List<Exception> getExceptions() throws SvException {
		List<Exception> exs = new ArrayList<Exception>();
		if (allDone()) {
			for (SvWriterThread swt : writerThreads) {
				if (swt.raisedException != null)
					exs.add(swt.raisedException);
			}
		} else
			throw (new SvException("system.err.thread_busy", svCONST.systemUser));
		return exs;
	}

	public void saveObject(DbDataArray items, boolean isBatch) throws SvException, InterruptedException {
		if (!isMTRunning.get() || threads.isEmpty())
			throw (new SvException("system.err.writer_not_running", svCONST.systemUser));
		if (!allDone())
			throw (new SvException("system.err.writer_busy", svCONST.systemUser));

		// if the total number of items is same size as number of threads, just use the
		// first thread
		int batchSize = items.size() > threads.size() ? items.size() / threads.size() : items.size();
		int start = 0;
		int end = batchSize;
		while (start < items.size()) {
			SvWriterThread swt = getFirstAvailable();
			if (swt != null) {
				if (swt.raisedException == null) {
					if (end > items.size())
						end = items.size();
					List<DbDataObject> range = items.getItems().subList(start, end);
					swt.saveObject(new DbDataArray(range), isBatch);
					start = end;
					end = start + batchSize;
					swt.offset = start;

				} else
					break;
			} else
				synchronized (isMTRunning) {
					isMTRunning.wait();
				}
		}
		// wait for all threads to finish
		while (!allDone())
			synchronized (isMTRunning) {
				isMTRunning.wait(10);
			}

		SvException svx = null;
		for (SvWriterThread swt : writerThreads) {
			if (swt.raisedException != null) {
				svx = swt.raisedException;
				break;
			}
		}
		if (svx != null)
			throw (svx);

	}

	public void shutdown() throws SvException, InterruptedException {
		if (isMTRunning.compareAndSet(true, false)) {
			boolean completeShut = false;
			while (!completeShut) {
				for (SvWriterThread swt : writerThreads) {
					swt.writer.release();
					swt.writer = null;
					swt.isRunning.set(false);
					synchronized (swt.isRunning) {
						swt.isRunning.notifyAll();
					}
				}
				Iterator<Thread> it = threads.iterator();
				while (it.hasNext()) {
					Thread t = it.next();
					t.join(5);
					if (!t.isAlive())
						it.remove();
				}
				if (threads.isEmpty())
					completeShut = true;
			}
		}
	}

	@Override
	public void close() throws Exception {
		shutdown();
	}
}
