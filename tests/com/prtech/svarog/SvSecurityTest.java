package com.prtech.svarog;

import static org.junit.Assert.fail;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.junit.BeforeClass;
import org.junit.Test;

import com.prtech.svarog_common.DbDataObject;

public class SvSecurityTest {
	static String testIp = "127.0.0.1";

	@BeforeClass
	public static void testpred() {

		try {
			testIp = SvUtil.getIpAdresses(false, ";");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testActivateExternal() {
		try {
			SvSecurity svs = new SvSecurity();
			DbDataObject user = svs.getUser("ADMIN");
			String uuid = user.getVal("USER_UID").toString();
			svs.activateExternalUser(uuid);
			System.out.println("activated");
		} catch (SvException e) {
			e.printStackTrace();
			fail(e.getLabelCode());
		}
	}

	@Test
	public void testUserSwitch() throws SvException {
		SvReader svc = new SvReader();
		svc.setInstanceUser(svCONST.serviceUser);

		SvSecurity svs = new SvSecurity(svc);
		svs.switchUser("ADMIN");
	}

	@Test
	public void testIpThrottle() {
		// lets reset the blocking cache
		SvSecurity.ipThrottle.invalidateAll();

		int maxRequest = 5;
		SvConf.setMaxRequestsPerMinute(maxRequest);
		try {
			SvSecurity svs = new SvSecurity(testIp);
			DbDataObject throttleObj = SvSecurity.ipThrottle.getIfPresent(testIp);
			if (1 != (int) throttleObj.getVal(Sv.Security.REQUEST_COUNT))
				fail("Test failed because of wrong request count");

			for (int i = 0; i < maxRequest - 1; i++) {
				svs = new SvSecurity(testIp);
			}
			// since we have saturated the number of max requests we should get an exception
			try {
				svs = new SvSecurity(testIp);
				fail("Test failed because the user was not throttled");
			} catch (SvException e) {
				if (!e.getLabelCode().equals(Sv.Exceptions.IP_BLOCKED))
					fail("Test failed with exception other than IP_BLOCKED");
			}
		} catch (SvException e) {
			e.printStackTrace();
			fail("Test failed with exception");
		}
		// DbDataObject throttleObj = SvSecurity.ipThrottle.getIfPresent(testIp);
		// throttleObj.set

	}

	@Test
	public void testIpThrottleReset() {
		// lets reset the blocking cache
		SvSecurity.ipThrottle.invalidateAll();

		int maxRequest = 1;
		SvConf.setMaxRequestsPerMinute(maxRequest);
		try {

			SvSecurity svs = new SvSecurity(testIp);
			DbDataObject throttleObj = SvSecurity.ipThrottle.getIfPresent(testIp);
			if (1 != (int) throttleObj.getVal(Sv.Security.REQUEST_COUNT))
				fail("Test failed because of wrong request count");

			// the block was reset
			SvSecurity.unblockPublicUser(testIp);
			try {
				svs = new SvSecurity(testIp);
			} catch (SvException e) {
				fail("Test failed with exception " + e.getLabelCode());
			}
		} catch (SvException | ExecutionException e) {
			e.printStackTrace();
			fail("Test failed with exception");
		}
		// DbDataObject throttleObj = SvSecurity.ipThrottle.getIfPresent(testIp);
		// throttleObj.set

	}

	@Test
	public void testIpThrottleIncreaseBlock() {
		// lets reset the blocking cache
		SvSecurity.ipThrottle.invalidateAll();

		int maxRequest = 1;
		SvConf.setMaxRequestsPerMinute(maxRequest);
		try {
			SvSecurity svs = new SvSecurity(testIp);
			DbDataObject throttleObj = SvSecurity.ipThrottle.getIfPresent(testIp);
			if (1 != (int) throttleObj.getVal(Sv.Security.REQUEST_COUNT))
				fail("Test failed because of wrong request count");

			// since we have saturated the number of max requests we should get an exception
			try {
				svs = new SvSecurity(testIp);
				fail("Test failed because the user was not throttled");
			} catch (SvException e) {
				if (!e.getLabelCode().equals(Sv.Exceptions.IP_BLOCKED))
					fail("Test failed with exception other than IP_BLOCKED");
			}

			if (1 != (int) throttleObj.getVal(Sv.Security.BLOCKED_COUNT))
				fail("Test failed because of wrong block count");

			DateTime firstBlockTime = new DateTime((DateTime) throttleObj.getVal(Sv.Security.BLOCKED_UNTIL));
			// since we have saturated the number of max requests we should get an exception
			try {
				svs = new SvSecurity(testIp);
				fail("Test failed because the user was not throttled");
			} catch (SvException e) {
				if (!e.getLabelCode().equals(Sv.Exceptions.IP_BLOCKED))
					fail("Test failed with exception other than IP_BLOCKED");
			}
			if (2 != (int) throttleObj.getVal(Sv.Security.BLOCKED_COUNT))
				fail("Test failed because of wrong block count");

			DateTime secondBlockTime = new DateTime((DateTime) throttleObj.getVal(Sv.Security.BLOCKED_UNTIL));

			Seconds dtper = Seconds.secondsBetween(firstBlockTime, secondBlockTime);
			if (!(dtper.getSeconds() > 58 && dtper.getSeconds() < 62))
				fail("Blacklist period not doubled!");
			// System.out.println(secondBlockTime);
		} catch (SvException e) {
			e.printStackTrace();
			fail("Test failed with exception");
		}
		// DbDataObject throttleObj = SvSecurity.ipThrottle.getIfPresent(testIp);
		// throttleObj.set

	}
}
