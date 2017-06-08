package cn.celloud.bf.stormProject1.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import cn.celloud.bf.stormProject1.constant.Constant;
public class HbaseImpl {
	// 声明静态配置
	private Configuration conf = null;

	public HbaseImpl(){
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZK_QUORUM);
	}
	
	/**
	 * 创建表
	 * @param tableName
	 * @param family
	 * @throws Exception
	 */
	public void createTable(String tableName, String[] family) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		//如果表存在，则不创建
		if (admin.tableExists(tableName)) {
			System.out.println("table Exists!");
			System.exit(0);
		} else {
			for (int i = 0; i < family.length; i++) {
				desc.addFamily(new HColumnDescriptor(family[i]));
			}
			admin.createTable(desc);
			System.out.println("create table Success!");
		}
	}

	/**
	 * 为表添加数据（2个列族，每个列族有多个列）
	 * @param rowKey
	 * @param tableName
	 * @param column1 第一个列族的列
	 * @param value1 值与上文一一对应
	 * @param column2 第二个列族的列
	 * @param value2 值与上文一一对应
	 * @throws IOException
	 */
	public void addData(String rowKey, String tableName, String[] column1, String[] value1, String[] column2,
			String[] value2) throws IOException {
		System.out.println("-------addData----------");
		// 设置rowkey
		Put put = new Put(Bytes.toBytes(rowKey));
		// table负责跟记录相关的操作如增删改查等
		// 获取表
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		// 获取所有的列族
		HColumnDescriptor[] columnFamilies = table.getTableDescriptor() 
				.getColumnFamilies();
		for (int i = 0; i < columnFamilies.length; i++) {
			// 获取列族名
			String familyName = columnFamilies[i].getNameAsString();
			System.out.println("=========familyName:"+familyName+"================");
			// article列族put数据
			if (familyName.equals(Constant.HBSAE_FAMILY_COLUMNS[0])) { 
				for (int j = 0; j < column1.length; j++) {
					put.add(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
				}
			}
			// author列族put数据
			if (familyName.equals(Constant.HBSAE_FAMILY_COLUMNS[1])) { 
				for (int j = 0; j < column2.length; j++) {
					put.add(Bytes.toBytes(familyName), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
				}
			}
		}
		table.put(put);
		System.out.println("add data Success!");
	}
	/**
	 * 为表添加数据（1个列族，每个列族有多个列）
	 * @param rowKey
	 * @param tableName
	 * @param column
	 * @param value
	 * @throws IOException
	 */
	public void addData(String rowKey, String tableName, String[] column, String[] value) throws IOException {
		System.out.println("-------addData----------");
		// 设置rowkey
		Put put = new Put(Bytes.toBytes(rowKey));
		// table负责跟记录相关的操作如增删改查等
		// 获取表
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		// 获取列族名
		String familyName = Constant.HBSAE_FAMILY_COLUMN;
		for (int j = 0; j < column.length; j++) {
			put.add(Bytes.toBytes(familyName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
		}
		table.put(put);
		System.out.println("add data Success!");
	}

	/**
	 * 根据rowkey查询
	 * @param tableName
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	public Result getResult(String tableName, String rowKey) throws IOException {
		System.out.println("--------查询tableName:"+tableName+",rowKey:"+rowKey+"的所有信息----------------");
		Get get = new Get(Bytes.toBytes(rowKey));
		// 获取表
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Result result = table.get(get);
		for (KeyValue kv : result.list()) {
			System.out.println("family:" + Bytes.toString(kv.getFamily()));
			System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
			System.out.println("value:" + Bytes.toString(kv.getValue()));
			System.out.println("Timestamp:" + kv.getTimestamp());
			System.out.println("-------------------------------------------");
		}
		return result;
	}

	/**
	 * 遍历查询hbase表
	 * @param tableName
	 * @throws IOException
	 */
	public void getResultScann(String tableName) throws IOException {
		System.out.println("------------开始查询所有表信息-----------------");
		Scan scan = new Scan();
		ResultScanner rs = null;
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		try {
			rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue kv : r.list()) {
					System.out.println("row:" + Bytes.toString(kv.getRow()));
					System.out.println("family:" + Bytes.toString(kv.getFamily()));
					System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
					System.out.println("value:" + Bytes.toString(kv.getValue()));
					System.out.println("timestamp:" + kv.getTimestamp());
					System.out.println("-------------------------------------------");
				}
			}
		} finally {
			rs.close();
		}
	}

	/**
	 * 遍历查询hbase表,设定rowKey的开始和结束位置
	 * @param tableName
	 * @param start_rowkey
	 * @param stop_rowkey
	 * @throws IOException
	 */
	public void getResultScann(String tableName, String start_rowkey, String stop_rowkey) throws IOException {
		System.out.println("---------------根据rowkey指定的起始和终止位置查询---------------------");
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(start_rowkey));
		scan.setStopRow(Bytes.toBytes(stop_rowkey));
		ResultScanner rs = null;
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		try {
			rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue kv : r.list()) {
					System.out.println("row:" + Bytes.toString(kv.getRow()));
					System.out.println("family:" + Bytes.toString(kv.getFamily()));
					System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
					System.out.println("value:" + Bytes.toString(kv.getValue()));
					System.out.println("timestamp:" + kv.getTimestamp());
					System.out.println("-------------------------------------------");
				}
			}
		} finally {
			rs.close();
		}
	}

	/**
	 * 指定列族和列查询信息
	 * @param tableName
	 * @param rowKey
	 * @param familyName
	 * @param columnName
	 * @throws IOException
	 */
	public void getResultByColumn(String tableName, String rowKey, String familyName, String columnName)
			throws IOException {
		System.out.println("-------查询tableName:"+tableName+",rowkey:"+rowKey+",familyName:"+familyName+",columnName:"+columnName+"----信息-------------");
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
		Result result = table.get(get);
		for (KeyValue kv : result.list()) {
			System.out.println("family:" + Bytes.toString(kv.getFamily()));
			System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
			System.out.println("value:" + Bytes.toString(kv.getValue()));
			System.out.println("Timestamp:" + kv.getTimestamp());
			System.out.println("-------------------------------------------");
		}
	}

	/**
	 * 更新表中的某一列
	 * @param tableName
	 * @param rowKey
	 * @param familyName
	 * @param columnName
	 * @param value 更新后的值
	 * @throws IOException
	 */
	public void updateTable(String tableName, String rowKey, String familyName, String columnName, String value)
			throws IOException {
		System.out.println("-------更新tableName:"+tableName+",rowkey:"+rowKey+",familyName:"+familyName+",columnName:"+columnName+",value:"+value+"--------------");
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
		table.put(put);
		System.out.println("update table Success!");
	}

	/**
	 * 查询某列数据的多个版本
	 * @param tableName
	 * @param rowKey
	 * @param familyName
	 * @param columnName
	 * @throws IOException
	 */
	public void getResultByVersion(String tableName, String rowKey, String familyName, String columnName)
			throws IOException {
		System.out.println("-------查询tableName:"+tableName+",rowkey:"+rowKey+",familyName:"+familyName+",columnName:"+columnName+"----全部版本信息-------------");
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
		get.setMaxVersions(5);
		Result result = table.get(get);
		for (KeyValue kv : result.list()) {
			System.out.println("family:" + Bytes.toString(kv.getFamily()));
			System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
			System.out.println("value:" + Bytes.toString(kv.getValue()));
			System.out.println("Timestamp:" + kv.getTimestamp());
			System.out.println("-------------------------------------------");
		}
	}

	/**
	 * 删除指定的列
	 * @param tableName
	 * @param rowKey
	 * @param falilyName
	 * @param columnName
	 * @throws IOException
	 */
	public void deleteColumn(String tableName, String rowKey, String familyName, String columnName)
			throws IOException {
		System.out.println("-------删除tableName:"+tableName+",rowkey:"+rowKey+",familyName:"+familyName+",columnName:"+columnName+"的值-------------");
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
		deleteColumn.deleteColumns(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
		table.delete(deleteColumn);
		System.out.println(familyName + ":" + columnName + "is deleted!");
	}

	/**
	 * 删除指定的rowkey对应的所有列
	 * @param tableName
	 * @param rowKey
	 * @throws IOException
	 */
	public void deleteAllColumn(String tableName, String rowKey) throws IOException {
		System.out.println("-----删除tablename:"+tableName+",rowKey:"+rowKey+"的所有信息---------");
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
		table.delete(deleteAll);
		System.out.println("all columns are deleted!");
	}

	/**
	 * 删除表
	 * @param tableName
	 * @throws IOException
	 */
	public void deleteTable(String tableName) throws IOException {
		System.out.println("----删除tablename:"+tableName+"的所有信息---");
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		System.out.println(tableName + "is deleted!");
	}
}
