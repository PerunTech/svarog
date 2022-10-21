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
package com.prtech.svarog_common;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * High Abstraction Class to implement basic calculations. The class aims to
 * implement the concept of calculating expression values like: (a + b)*c
 * +(d-n)/f
 * 
 * The class will not implement operator presedence
 * 
 * @author ristepejov
 *
 */
public abstract class SvCalculator {
	private static final Logger log4j = LogManager.getLogger(SvCalculator.class.getName());

	/**
	 * Mathematical operator type, to specify the operation executed between two
	 * calculators on the same level in the expression tree
	 * 
	 * @author ristepejov
	 *
	 */
	public enum SvCalcOperator {
		ADD, SUBSTRACT, MULTIPLY, DIVIDE
	};

	/**
	 * Resulting value of the calculation
	 */
	BigDecimal value = new BigDecimal(0);

	/**
	 * Name of the calculator. Used for logging and identification purposes
	 */

	String calcName = getCalculatorName();
	/**
	 * List of sub calculations used to form the expression. These are
	 * effectively calculation branches of the tree
	 */
	ArrayList<SvCalculator> subCalculations = null;

	/**
	 * The mathematical operation between this and the next calculation
	 */
	SvCalcOperator nextCalcOperand = SvCalcOperator.ADD;

	/**
	 * Paramters used for parameterising the calculation it self
	 */
	final public HashMap<String, Object> params = new HashMap<String, Object>() ;

	/**
	 * Reference to the parent calculator
	 */
	SvCalculator parent = null;

	/**
	 * Reference to the root calculator
	 */
	SvCalculator root = null;

	/**
	 * Getter for the list of sub calculations
	 */
	ArrayList<SvCalculator> getSubCalculations() {
		if (subCalculations == null)
			subCalculations = new ArrayList<SvCalculator>();
		return subCalculations;
	}

	public abstract String getCalculatorName();

	/**
	 * Method for doing the real calculation of the final resulting value.
	 * 
	 * @param currentValue
	 *            The current value of the calculator.
	 * @return the new value of the calculator
	 */
	public abstract BigDecimal doCalc(BigDecimal currentValue);
	
	
	/**
	 * Method for loading starting values from params
	 * 
	 */
	public abstract void initCalc();
	

	/**
	 * Method for testing if the calculation is feasible at all. If it the
	 * return value is false, the standard process preCalc(), doCalc() and
	 * postCalc() will not be executed at all
	 * 
	 * @return Boolean flag to let the calculator know if it should calculate at
	 *         all
	 */
	public abstract boolean shouldCalc();

	/**
	 * Method for preparations of calculation, before executing doCalc.
	 * 
	 * @param currentValue
	 *            The current value of the calculator. At this stage should be
	 *            0.
	 * @return the new value of the calculator
	 */
	public abstract BigDecimal preCalc(BigDecimal currentValue);

	/**
	 * Method for post-processing of calculation, after executing doCalc.
	 * 
	 * @param currentValue
	 *            The current value of the calculator.
	 * @return the new value of the calculator
	 */
	public abstract BigDecimal postCalc(BigDecimal currentValue);
	
	
	/**
	 * Method for saving the calculation, after executing postCalc().
	 * 
	 */
	public abstract void saveCalc();


	/**
	 * Method for adding new child(sub) Calculator
	 * 
	 * @param subCalc
	 *            The sub calculator it self
	 * @throws Exception
	 *             If the reference equals this or it already exists in the list
	 *             or if the calculator was already assigned to a different node
	 *             in the tree, exception is thrown
	 */
	public void addSubCalc(SvCalculator subCalc) throws Exception {

		if (!subCalc.equals(this) && getSubCalculations().indexOf(subCalc) < 0 && subCalc.getParent() == null
				&& subCalc.getRoot() == null) {

			subCalc.parent = this;
			subCalc.root = this.root == null ? this : this.root;

			getSubCalculations().add(subCalc);
		} else
			throw (new Exception("system.error.sub_calc_error"));
	}

	/**
	 * Method for removing existing child(sub) Calculator
	 * 
	 * @param subCalc
	 *            The sub calculator it self
	 * @throws Exception
	 *             If the reference does not exists in the list, exception is
	 *             thrown
	 */
	public void removeSubCalc(SvCalculator subCalc) throws Exception {

		if (!subCalc.equals(this) && getSubCalculations().indexOf(subCalc) > 0)
			getSubCalculations().remove(subCalc);
		else
			throw (new Exception("system.error.sub_calc_notexist"));
	}

	/**
	 * Method to perform a full calculation of the calculator it self. If the
	 * calculator has sub calculations, then it will iterate them and calculate
	 * each one while performing the Next Calculation Operand between every
	 * result. If there are no sub calculations the doCalc method will be
	 * executed.
	 * 
	 * @return The calculated value
	 */
	public BigDecimal calculate() {
		if (log4j.isDebugEnabled())
			log4j.debug("Calculator:" + calcName + ", initCalc() started");
		initCalc();
		if (log4j.isDebugEnabled())
			log4j.debug("Calculator:" + calcName + ", initCalc() finished");
		if (shouldCalc()) {
			if (log4j.isDebugEnabled())
				log4j.debug("Calculator:" + calcName + ", preCalc() execution with value:" + value.toString());
			value = preCalc(value);
			if (log4j.isDebugEnabled())
				log4j.debug("Calculator:" + calcName + ", preCalc() finished with value:" + value.toString());

			if (subCalculations != null && subCalculations.size() > 0) {
				if (log4j.isDebugEnabled())
					log4j.debug(
							"Calculator:" + calcName + ", executing " + subCalculations.size() + "sub calculations ");

				SvCalcOperator prevOperand = null;
				for (SvCalculator svc : subCalculations) {
					BigDecimal tmpValue = svc.calculate();
					if (prevOperand == null) {
						value = tmpValue;
						if (log4j.isDebugEnabled())
							log4j.debug("Calculator:" + calcName + ", first sub calculator finished with value"
									+ value.toString());
					} else {
						if (log4j.isDebugEnabled())
							log4j.debug(
									"Calculator:" + calcName + ", sub calculator operation:" + prevOperand.toString());

						switch (prevOperand) {
						case ADD:
							value = value.add(tmpValue);
							break;
						case SUBSTRACT:
							value = value.subtract(tmpValue);
							break;
						case MULTIPLY:
							value = value.multiply(tmpValue);
							break;
						case DIVIDE:
							value = value.divide(tmpValue);
							break;
						}
						if (log4j.isDebugEnabled())
							log4j.debug("Calculator:" + calcName + ", sub calculator temp value:" + value.toString());
					}
					prevOperand = svc.getNextCalcOperand();
				}
			} else {
				if (log4j.isDebugEnabled())
					log4j.debug("Calculator:" + calcName + ", doCalc() executing with value:" + value.toString());
				value = doCalc(value);
				if (log4j.isDebugEnabled())
					log4j.debug("Calculator:" + calcName + ", doCalc() finished with value:" + value.toString());

			}
			if (log4j.isDebugEnabled())
				log4j.debug("Calculator:" + calcName + ", postCalc() executing with value:" + value.toString());
			value = postCalc(value);
			if (log4j.isDebugEnabled())
				log4j.debug("Calculator:" + calcName + ", postCalc() finished with value:" + value.toString());
		} else if (log4j.isDebugEnabled())
			log4j.debug("Calculator:" + calcName + ", shouldCalc() returned false");

		if (log4j.isDebugEnabled())
			log4j.debug("Calculator:" + calcName + ", saveCalc() started:" + value.toString());
		saveCalc();
		if (log4j.isDebugEnabled())
			log4j.debug("Calculator:" + calcName + ", saveCalc() sucess");

		if (log4j.isDebugEnabled())
			log4j.debug("Calculator:" + calcName + ", final value:" + value.toString());
		return value;
	}

	public SvCalcOperator getNextCalcOperand() {
		return nextCalcOperand;
	}

	public void setNextCalcOperand(SvCalcOperator nextCalcOperand) {
		this.nextCalcOperand = nextCalcOperand;
	}


	public BigDecimal getValue() {
		return value;
	}

	public SvCalculator getParent() {
		return parent;
	}

	public SvCalculator getRoot() {
		return root;
	}

	public String getCalcName() {
		return calcName;
	}

	public void setCalcName(String calcName) {
		this.calcName = calcName;
	}

}
