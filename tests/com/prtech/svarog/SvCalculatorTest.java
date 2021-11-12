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

import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.Scanner;

import org.junit.Test;

import com.prtech.svarog_common.SvCalculator;
import com.prtech.svarog_common.SvCalculator.SvCalcOperator;

/**
 * @author PR01
 * 
 */
public class SvCalculatorTest {

	public class SvCalculatorSubA extends SvCalculator {

		@Override
		public BigDecimal doCalc(BigDecimal currentValue) {
			// TODO Auto-generated method stub
			String retVal; retVal = "4";
			//Scanner scanner = new Scanner(System.in);
			System.out.println("Please enter" + getCalcName()+":"+retVal);
			
			BigDecimal ret = new BigDecimal(retVal);
			//scanner.close();
			return ret;
		}

		@Override
		public void initCalc() {
			// TODO Auto-generated method stub
		}

		@Override
		public void saveCalc() {
			// TODO Auto-generated method stub
		}

		@Override
		public boolean shouldCalc() {
			// TODO Auto-generated method stub
			return true;
		}

		@Override
		public BigDecimal preCalc(BigDecimal currentValue) {
			// we don't pre-calc.
			return currentValue;
		}

		@Override
		public BigDecimal postCalc(BigDecimal currentValue) {
			// we don't post-calc.
			return currentValue;
		}

		@Override
		public String getCalculatorName() {
			// TODO Auto-generated method stub
			return "AVal";
		}

	}

	public class SvCalculatorExpression extends SvCalculator {

		@Override
		public void initCalc() {
			// TODO Auto-generated method stub
		}

		@Override
		public void saveCalc() {
			// TODO Auto-generated method stub
		}

		@Override
		public BigDecimal doCalc(BigDecimal currentValue) {
			return currentValue;
		}

		@Override
		public boolean shouldCalc() {
			// TODO Auto-generated method stub
			return true;
		}

		@Override
		public BigDecimal preCalc(BigDecimal currentValue) {
			// we don't pre-calc.
			return currentValue;
		}

		@Override
		public BigDecimal postCalc(BigDecimal currentValue) {
			// we don't post-calc.
			return currentValue;
		}

		@Override
		public String getCalculatorName() {
			// TODO Auto-generated method stub
			return "FullExpression";
		}

	}

	public class SvCalculatorSubB extends SvCalculator {

		@Override
		public BigDecimal doCalc(BigDecimal currentValue) {
			// TODO Auto-generated method stub
			
			Scanner scanner = new Scanner(System.in);
			String retVal = "5.2";
			System.out.println("Please enter" + getCalcName()+":"+retVal);
			BigDecimal ret = new BigDecimal(retVal);
			//scanner.close();
			return ret;
		}

		@Override
		public void initCalc() {
			// TODO Auto-generated method stub
		}

		@Override
		public void saveCalc() {
			// TODO Auto-generated method stub
		}

		@Override
		public boolean shouldCalc() {
			// TODO Auto-generated method stub
			return true;
		}

		@Override
		public BigDecimal preCalc(BigDecimal currentValue) {
			// we don't pre-calc.
			return currentValue;
		}

		@Override
		public BigDecimal postCalc(BigDecimal currentValue) {
			// we don't post-calc.
			if (currentValue.compareTo(new BigDecimal(100)) > 0)
				currentValue = currentValue.multiply(new BigDecimal(0.8));

			return currentValue;
		}

		@Override
		public String getCalculatorName() {
			// TODO Auto-generated method stub
			return "BVal";
		}

	}

	public class SvCalculatorSubC extends SvCalculator {

		@Override
		public BigDecimal doCalc(BigDecimal currentValue) {
			// TODO Auto-generated method stub
			String retVal;
			retVal = "7";
			System.out.println("Please enter" + getCalcName()+":"+retVal);
			
			BigDecimal ret = new BigDecimal(retVal);
			//scanner.close();
			return ret;
		}

		@Override
		public void initCalc() {
			// TODO Auto-generated method stub
		}

		@Override
		public void saveCalc() {
			// TODO Auto-generated method stub
		}

		@Override
		public boolean shouldCalc() {
			// TODO Auto-generated method stub
			return true;
		}

		String companyName;

		@Override
		public BigDecimal preCalc(BigDecimal currentValue) {
			// we don't pre-calc.
			String retVal;
			Scanner scanner = new Scanner(System.in);
			System.out.println("Please enter company name:PERUN");
			companyName = "PERUN.TECH";
			//scanner.close();
			return currentValue;
		}

		@Override
		public BigDecimal postCalc(BigDecimal currentValue) {
			// we don't post-calc.
			if (companyName.startsWith("PERUN"))
				currentValue = currentValue.multiply(new BigDecimal(2));
			return currentValue;
		}

		@Override
		public String getCalculatorName() {
			// TODO Auto-generated method stub
			return "CVal";

		}

	}

	@Test
	public void SvCalculatorTest1() {
		System.out.println("Goal is to calculate the expression ((a+b)*c)-(b2*d)");
		System.out.println("The value B will max at 100. After 100 it will be lowered to 80% of the initial value");
		System.out.println("The value C will be doubled if your company Name is PERUN.");

		SvCalculator svcRoot = new SvCalculatorExpression();
		SvCalculator svcAB = new SvCalculatorExpression();
		svcAB.setCalcName("A+B");
		svcAB.setNextCalcOperand(SvCalcOperator.MULTIPLY);
		SvCalculator svcABC = new SvCalculatorExpression();
		svcABC.setNextCalcOperand(SvCalcOperator.SUBSTRACT);
		SvCalculator svcBD = new SvCalculatorExpression();
		//svcABC.setCalcName("(A+B)*C");
		svcABC.setCalcName(svcAB.getCalcName()+svcAB.getNextCalcOperand()+"C");
		svcBD.setCalcName("(B2*D");

		SvCalculator svcA = new SvCalculatorSubA();
		SvCalculator svcB = new SvCalculatorSubB();
		SvCalculator svcC = new SvCalculatorSubC();
		// A and D calculators are the same. just rename it
		SvCalculator svcD = new SvCalculatorSubA();
		svcD.setCalcName("DVal");
		SvCalculator svcB2 = new SvCalculatorSubB();
		svcB2.setCalcName("B2Val");
		svcB2.setNextCalcOperand(SvCalcOperator.MULTIPLY);

		try {
			svcAB.addSubCalc(svcA);
			svcAB.addSubCalc(svcB);

			svcABC.addSubCalc(svcAB);
			svcABC.addSubCalc(svcC);

			svcBD.addSubCalc(svcB2);
			svcBD.addSubCalc(svcD);

			// add A+B as one and B+D as second expression
			svcRoot.addSubCalc(svcABC);
			svcRoot.addSubCalc(svcBD);

			System.out.println("Final value:" + svcRoot.calculate().toString());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
