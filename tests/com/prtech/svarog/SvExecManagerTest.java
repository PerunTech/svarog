package com.prtech.svarog;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_interfaces.ISvCore;
import com.prtech.svarog_interfaces.ISvExecutor;
import com.prtech.svarog_interfaces.ISvExecutorGroup;

public class SvExecManagerTest {
	@BeforeClass
	public static void init() {
		try {
			SvCore.initSvCore();
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	static final Logger log4j = SvConf.getLogger(SvExecManagerTest.class);
	private final String category = "TEST";
	private final String name = "TESTEXEC";

	public class TestExecutorGroup implements ISvExecutorGroup {

		// final Logger log4j = SvConf.getLogger(TestExecutor.class);

		private final String description = "Text executor group";
		private final DateTime start = new DateTime("2012-12-31T00:00:00+00");
		private final DateTime end = new DateTime("9999-12-31T00:00:00+00");
		private final Class<?> type = DbDataObject.class;

		@Override
		public String getCategory() {
			return category;
		}

		@Override
		public DateTime getStartDate() {
			return start;
		}

		@Override
		public DateTime getEndDate() {
			return end;
		}

		@Override
		public long versionUID() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Map<String, Class<?>> getReturningTypes() {
			// TODO Auto-generated method stub
			Map<String, Class<?>> retval = new HashMap<String, Class<?>>();
			retval.put(name + "g1", type);
			retval.put(name + "g2", type);
			return retval;
		}

		@Override
		public List<String> getNames() {
			ArrayList<String> retval = new ArrayList<String>();
			retval.add(name + "g1");
			retval.add(name + "g2");
			return retval;

		}

		@Override
		public Map<String, String> getDescriptions() {
			Map<String, String> retval = new HashMap<String, String>();
			retval.put(name + "g1", description + "g1");
			retval.put(name + "g2", description + "g2");
			return retval;
		}

		@Override
		public Object execute(String name, Map<String, Object> params, ISvCore svCore) throws SvException {
			// TODO Auto-generated method stub
			log4j.info("Method called:" + name + " params:" + (params != null ? params.toString() : "no params"));
			return null;
		}

	}

	public class TestExecutor implements ISvExecutor {

		// final Logger log4j = SvConf.getLogger(TestExecutor.class);

		private final String description = "Text executor";
		private final DateTime start = new DateTime("2012-12-31T00:00:00+00");
		private final DateTime end = new DateTime("9999-12-31T00:00:00+00");
		private final Class<?> type = DbDataObject.class;
		private final String exName;

		TestExecutor(String executorName) {
			if (executorName != null)
				this.exName = executorName;
			else
				this.exName = name;
		}

		@Override
		public Class<?> getReturningType() {
			return type;
		}

		@Override
		public String getCategory() {
			return category;
		}

		@Override
		public String getName() {
			return exName;
		}

		@Override
		public String getDescription() {
			return description;
		}

		@Override
		public DateTime getStartDate() {
			return start;
		}

		@Override
		public DateTime getEndDate() {
			return end;
		}

		@Override
		public long versionUID() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Object execute(Map<String, Object> params, ISvCore svCore) throws SvException {
			// TODO Auto-generated method stub
			log4j.info("Executor called:" + (params != null ? params.toString() : "no params"));
			return getName();
		}
	}

	@Test
	public void callExecutor() {
		SvExecManager sve = null;
		try {
			sve = new SvExecManager();
			sve.osgiServices = new Object[1];
			sve.osgiServices[0] = new TestExecutor(null);
			sve.execute(category, name, null, null);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Test raised execption" + e.toString());
		} finally {
			if (sve != null)
				sve.release();
		}

		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void callExecutorGroup() {
		SvExecManager sve = null;
		try {
			sve = new SvExecManager();
			sve.osgiServices = new Object[2];
			sve.osgiServices[0] = new TestExecutor(null);
			sve.osgiServices[1] = new TestExecutorGroup();
			sve.execute(category, name + "g2", null, null);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Test raised execption" + e.toString());
		} finally {
			if (sve != null)
				sve.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void callInvalidExecutor() {
		SvExecManager sve = null;
		SvReader svr = null;
		SvWorkflow svw = null;
		try {
			sve = new SvExecManager();
			svr = new SvReader(sve);
			svw = new SvWorkflow(sve);
			sve.osgiServices = new Object[1];
			ISvExecutor svExec = new TestExecutor(null);
			sve.osgiServices[0] = svExec;
			sve.execute(category, name, null, null);

			DbSearchExpression search = new DbSearchExpression();
			search.addDbSearchItem(new DbSearchCriterion("NAME", DbCompareOperand.EQUAL, svExec.getName()));
			search.addDbSearchItem(new DbSearchCriterion("CATEGORY", DbCompareOperand.EQUAL, svExec.getCategory()));
			search.addDbSearchItem(new DbSearchCriterion("VERSION", DbCompareOperand.EQUAL, svExec.versionUID()));

			DbQueryObject dqo = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_EXECUTORS), search, null, null);
			DbDataArray objects = svr.getObjects(dqo, 0, 0);
			if (objects.size() < 1)
				fail("Can't find executor");
			DbDataObject dbexec = objects.get(0);

			svw.setAutoCommit(true);
			svw.moveObject(dbexec, "INVALID");

			String errMessage = null;
			try {
				sve.execute(category, name, null, null);
				errMessage = "No exception was raised!";
			} catch (SvException ex) {
				if (!ex.getLabelCode().equals("system.error.executor_not_valid")) {
					ex.printStackTrace();
					errMessage = "Test raised wrong exception. Should invalid executor";
				}

			}
			svw.moveObject(dbexec, "VALID");
			if (errMessage != null)
				fail(errMessage);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Test raised execption" + e.toString());
		} finally {
			if (sve != null)
				sve.release();
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void callExecutorPastEndDate() {
		SvExecManager sve = null;
		SvReader svr = null;
		SvWriter svw = null;
		String errMessage = null;
		try {
			sve = new SvExecManager();
			svr = new SvReader(sve);
			svw = new SvWriter(sve);
			sve.osgiServices = new Object[1];
			ISvExecutor svExec = new TestExecutor(null);
			sve.osgiServices[0] = svExec;
			sve.execute(category, name, null, null);

			DbSearchExpression search = new DbSearchExpression();
			search.addDbSearchItem(new DbSearchCriterion("NAME", DbCompareOperand.EQUAL, svExec.getName()));
			search.addDbSearchItem(new DbSearchCriterion("CATEGORY", DbCompareOperand.EQUAL, svExec.getCategory()));
			search.addDbSearchItem(new DbSearchCriterion("VERSION", DbCompareOperand.EQUAL, svExec.versionUID()));

			DbQueryObject dqo = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_EXECUTORS), search, null, null);
			DbDataArray objects = svr.getObjects(dqo, 0, 0);
			if (objects.size() < 1)
				fail("Can't find executor");
			DbDataObject dbexec = objects.get(0);

			svw.setAutoCommit(true);
			dbexec.setVal("END_DATE", new DateTime());
			svw.saveObject(dbexec);
			Thread.sleep(100); // to ensure end date has passed

			try {
				sve.execute(category, name, null, null);
				errMessage = "No exception was raised!";
			} catch (SvException ex) {
				if (!ex.getLabelCode().equals("system.err.exec_not_found")) {
					ex.printStackTrace();
					errMessage = "Test raised wrong exception. Should invalid executor";
				}

			}
			dbexec.setVal("END_DATE", SvConf.MAX_DATE);
			svw.saveObject(dbexec);

			if (errMessage != null)
				fail(errMessage);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Test raised execption" + e.toString());
		} finally {
			if (sve != null)
				sve.release();
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void getExecutorPackItems() {

		try (SvExecManager sve = new SvExecManager()) {
			sve.setAutoCommit(false);
			// first assign an array of 3 virtual executors
			sve.osgiServices = new Object[3];

			// create 3 different executors
			sve.osgiServices[0] = new TestExecutor(name + "1");
			sve.osgiServices[1] = new TestExecutor(name + "2");
			sve.osgiServices[2] = new TestExecutor(name + "3");

			// now create the packages. The 'test.pack' is the mother package, and
			// 'test.pack.child' is inheriting from the mother pack
			DbDataObject rootPack = sve.createExecutorPack("test.pack", null);
			DbDataObject childPack = sve.createExecutorPack("test.pack.child", null, rootPack.getObjectId());

			// we assign 2 executors to the mother package
			sve.createExecutorPackItem(rootPack, "test.pack.item", (ISvExecutor) sve.osgiServices[0]);
			sve.createExecutorPackItem(rootPack, "test.pack.item1", (ISvExecutor) sve.osgiServices[1]);

			// assign 1 more executor to the child. The executor assigned to the child, has
			// the same name as the first executor of mother pack
			// having the same name, the child executor named 'test.pack.item' will hide the
			// same executor coming from the mother pack.
			sve.createExecutorPackItem(childPack, "test.pack.item", (ISvExecutor) sve.osgiServices[2]);

			// now test, get all executors from the mother pack. We should get 2
			// ("test.pack.item" and "test.pack.item1")
			Map<String, ISvExecutor> rootItems = sve.getExecutorPackItems(rootPack, null);

			// get all executors from the child pack (we should have 2) - ("test.pack.item"
			// and "test.pack.item1")
			// but the "test.pack.item" in the child shall have reference to
			// sve.osgiServices[2] instead of sve.osgiServices[0] which is the root pack
			Map<String, ISvExecutor> childItems = sve.getExecutorPackItems(childPack, null);

			// do we have the test pack item in the mother package
			if (!rootItems.get("test.pack.item").equals(sve.osgiServices[0]))
				fail("Non matching root item");

			// do we have the test pack item 1 in the mother package
			if (!rootItems.get("test.pack.item1").equals(sve.osgiServices[1]))
				fail("Non matching root item1");

			// have the child package overriden the test.pack.item from the mother package?
			if (!childItems.get("test.pack.item").equals(sve.osgiServices[2]))
				fail("Non matching root item");

			// has the child inherited the test.pack.item from the mother package.
			if (!childItems.get("test.pack.item1").equals(sve.osgiServices[1]))
				fail("Non matching root item1");

			Object o = sve.executePack("test.pack.child", "test.pack.item", null, null);
			if (!o.toString().equals(name + "3"))
				fail("Executor pack by pack/item label failed");

			o = sve.executePack("test.pack", "test.pack.item", null, null);
			if (!o.toString().equals(name + "1"))
				fail("Executor pack by pack/item label failed");

			
			sve.dbRollback();
		} catch (Exception e) {
			e.printStackTrace();
			fail("Test raised execption" + e.toString());
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}
}
