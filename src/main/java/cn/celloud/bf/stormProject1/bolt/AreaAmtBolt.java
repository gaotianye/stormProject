package cn.celloud.bf.stormProject1.bolt;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.celloud.bf.stormProject1.constant.Constant;
import cn.celloud.bf.stormProject1.hbase.HbaseImpl;
import cn.celloud.bf.utils.CaculateUtils;
import cn.celloud.bf.utils.DateUtils;
/**
 * bolt重启/初始化时，需要从hbase中读取数据
 * 问：为什么在这个bolt里面写呢？
 * 答：只有从此类中才开始进行计算。
 * 
 * spout-->filter bolt-->amt bolt-->result bolt
 * 发射源             过滤                                  开始计算                     汇总计算
 * @author Administrator
 *
 */
public class AreaAmtBolt extends BaseRichBolt {
	Logger logger = LoggerFactory.getLogger(AreaAmtBolt.class);
	
	private static final long serialVersionUID = 1L;
	
	private OutputCollector collector;
	private Map<String,Double> map = null;
	private HbaseImpl hbase;
	private String today;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		map = new HashMap<String,Double>();
		hbase = new HbaseImpl();
		today = DateUtils.formatDate(new Date());	
		//初始化map
		System.out.println("===========初始化map============");
		map = initMap(today, hbase);
		for (Entry<String, Double> entry : map.entrySet()) {
			System.out.println("rowkey:"+entry.getKey()+",amt:"+entry.getValue());
		}
	}

	public void execute(Tuple input) {
		Double order_amt = input.getDoubleByField("order_amt");
		String create_time = input.getStringByField("create_time");
		Integer area_id = input.getIntegerByField("area_id");
		if(order_amt!=null && create_time!=null && area_id!=null){
			//例如：2017-06-07_1
			String date_area = create_time+"_"+area_id;
			Double amt = map.get(date_area);
			if(amt==null){
				amt = 0.0;
			}
//			logger.info("new:{},old:{}",order_amt,amt);
			amt = CaculateUtils.add(amt, order_amt);
//			logger.info("caculate:{}",amt);
			map.put(date_area, amt);
			//2017-06-07_1   55.55
//			logger.info("date_area:{},amt:{}",date_area,amt);
			collector.emit(new Values(date_area, amt));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date_area","amt"));
	}
	
	/**
	 * 从hbase中获取数据
	 * @param rowKeyDate
	 * @param hbase
	 * @return
	 */
	public Map<String,Double> initMap(String rowKeyLike,HbaseImpl hbase){
		//"date_area","amt"
		Map<String,Double> map = new HashMap<String,Double>();
		String tableName = Constant.HBASE_TB_NAME;
		ResultScanner listResult = null;
		try {
			listResult = hbase.getListResult(tableName, rowKeyLike);
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (Result result : listResult) {
			for (KeyValue kv : result.list()) {
				String date_area = Bytes.toString(kv.getRow());
				Double amt = Double.parseDouble(Bytes.toString(kv.getValue()));
				map.put(date_area, amt);
			}
		}
		return map;
	}

}
