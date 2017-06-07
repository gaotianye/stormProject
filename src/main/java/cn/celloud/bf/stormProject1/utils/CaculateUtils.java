package cn.celloud.bf.stormProject1.utils;

import java.math.BigDecimal;
import java.text.DecimalFormat;

public class CaculateUtils {
	/**
	 * 加法
	 * @param a
	 * @param b
	 * @return
	 */
	public static Double add(Double a,Double b){
		DecimalFormat df = new DecimalFormat("#0.00");   
		return Double.parseDouble(df.format(a+b));
	}
	
	/**
	 * 减法
	 * @param a
	 * @param b
	 * @return
	 */
	public static Double sub(Double a,Double b){
		DecimalFormat df = new DecimalFormat("#0.00");   
		return Double.parseDouble(df.format(a-b));
	}
	
	/**
	 * 乘法
	 * @param a
	 * @param b
	 * @return
	 */
	public static Double mul(Double a,Double b){
		DecimalFormat df = new DecimalFormat("#0.00");   
		return Double.parseDouble(df.format(a*b));
	}
	
	/**
	 * 除法
	 * @param a
	 * @param b
	 * @return
	 */
	public static Double div(Double a,Double b,int len){
		DecimalFormat df = new DecimalFormat("#0.00");   
		return Double.parseDouble(df.format(a/b));
	}
}
