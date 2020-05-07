package com.prtech.svarog;

import java.util.Map;

import org.joda.time.DateTime;
import com.prtech.svarog_interfaces.ISvCore;
import com.prtech.svarog_interfaces.ISvExecutor;
import com.prtech.svarog_interfaces.ISvExecutorGroup;

public class SvExecutorWrapper implements ISvExecutor {
	final ISvExecutorGroup executorGroup;
	final String methodName;

	SvExecutorWrapper(ISvExecutorGroup executorGroup, String methodName) {
		if (executorGroup == null || methodName == null || methodName.isEmpty())
			throw new NullPointerException("ExecutorGroup and Method Name can not be null");
		this.executorGroup = executorGroup;
		this.methodName = methodName;
	}

	@Override
	public long versionUID() {
		return executorGroup.versionUID();
	}

	@Override
	public Class<?> getReturningType() {
		return executorGroup.getReturningTypes().get(methodName);
	}

	@Override
	public String getCategory() {
		return executorGroup.getCategory();
	}

	@Override
	public String getName() {
		return methodName;
	}

	@Override
	public String getDescription() {
		return executorGroup.getDescriptions().get(methodName);
	}

	@Override
	public DateTime getStartDate() {
		return executorGroup.getStartDate();
	}

	@Override
	public DateTime getEndDate() {
		return executorGroup.getEndDate();
	}

	@Override
	public Object execute(Map<String, Object> params, ISvCore svCore) throws SvException {
		return executorGroup.execute(methodName, params, svCore);
	}

}
