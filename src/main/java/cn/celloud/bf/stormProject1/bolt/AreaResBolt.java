package cn.celloud.bf.stormProject1.bolt;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.celloud.bf.stormProject1.constant.Constant;
import cn.celloud.bf.stormProject1.hbase.HbaseImpl;
/**
 * 注意：此bolt中使用了定时任务
 * 在execute中使用时，一定要先判断是否是系统级别的id。
 * 否则，当input.getStringByField("xxx")，会报 xxx不存在。
 * 这是因为，fields此时为rate_secs，当然找不到xxx
 * 
 * @author Administrator
 *
 */
public class AreaResBolt extends BaseRichBolt {
	Logger logger = LoggerFactory.getLogger(AreaResBolt.class);
	
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	private HashMap<String, Double> hashMap = null;
	private HbaseImpl hbase = null;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		hashMap = new HashMap<String, Double>();
		hbase = new HbaseImpl(); 
	}

	public void execute(Tuple input) {
		String date_area = null;
		Double amt = null;
		String tbName = Constant.HBASE_TB_NAME;
		String[] column = Constant.HBASE_COLMN;
		//间隔5s执行一次
		if(input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)){
			for (String key : hashMap.keySet()) {
				//key:2017-06-08_1    value:200.50
				String[] value = null; 
				value[0] = hashMap.get(key)+"";
				try {
					hbase.addData(key,tbName,column,value);
				} catch (IOException e) {
					e.printStackTrace();
				}
//				logger.info("date_area:{},amt:{}",key,hashMap.get(key));
			}
		}else{
			date_area = input.getStringByField("date_area");
			amt = input.getDoubleByField("amt");
			hashMap.put(date_area,amt);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
	
	/**
	 * 设置定时任务，每隔5s钟往hbase中put数据
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		hashMap.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
		return hashMap;
	}

}
