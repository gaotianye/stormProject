package cn.celloud.bf.stormProject1.hbase;

import cn.celloud.bf.stormProject1.constant.Constant;
import cn.celloud.bf.stormProject1.hbase.HbaseImpl;

public class HbaseTest {
	public static void main(String[] args) throws Exception {
		HbaseImpl hbase = new HbaseImpl();
		// 创建表
		String tableName = Constant.HBASE_TB_NAME;
		String[] family = Constant.HBSAE_FAMILY_COLUMNS;
		hbase.creatTable(tableName, family);

		String[] column1 = Constant.HBASE_COLMN_1;
		String[] rowkey1_value1 = { "gaotianye","gao_password","male" };
		String[] rowkey2_value1 = { "yuwei","yu_password","male" };
		String[] rowkey3_value1 = { "zhangshuang","zhang_password","female" };
		String[] rowkey4_value1 = { "lisi","li_password","male" };

		String[] column2 = Constant.HBASE_COLMN_2;
		String[] rowkey1_value2 = { "beijing", "chaohu" };
		String[] rowkey2_value2 = { "hebei", "songyuan" };
		String[] rowkey3_value2 = { "shanghai", "changchun" };
		String[] rowkey4_value2 = { "wuhan", "zhengzhou" };

		hbase.addData("rowkey1", tableName, column1, rowkey1_value1, column2, rowkey1_value2);
		hbase.addData("rowkey2", tableName, column1, rowkey2_value1, column2, rowkey2_value2);
		hbase.addData("rowkey3", tableName, column1, rowkey3_value1, column2, rowkey3_value2);
		hbase.addData("rowkey4", tableName, column1, rowkey4_value1, column2, rowkey4_value2);
		
		// 遍历查询 
		hbase.getResultScann(tableName); 
		// 根据rowkey范围遍历查询 
		hbase.getResultScann(tableName, "rowkey4", "rowkey5"); 
		// 查询
		hbase.getResult(tableName, "rowkey1"); 
		// 查询某一列的值 
		hbase.getResultByColumn(tableName,"rowkey1", "cf1", "name"); 
		// 更新列
		hbase.updateTable(tableName, "rowkey1","cf1", "name", "xxx"); 
		// 再次查询某一列的值 
		hbase.getResultByColumn(tableName,"rowkey1", "cf1", "name"); 
		// 查询某列的多版本
		hbase.getResultByVersion(tableName,"rowkey1", "cf1", "name"); 
		// 删除一列 
		hbase.deleteColumn(tableName,"rowkey1", "cf1", "sex"); 
		// 遍历查询 
		hbase.getResultScann(tableName); 
		// 删除所有列 
		hbase.deleteAllColumn(tableName,"rowkey2"); 
		// 遍历查询 
		hbase.getResultScann(tableName); 
		// 删除表 
//		hbase.deleteTable(tableName);
	}
}
