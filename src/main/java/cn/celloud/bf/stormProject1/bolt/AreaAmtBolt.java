package cn.celloud.bf.stormProject1.bolt;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.celloud.bf.utils.CaculateUtils;
import cn.celloud.bf.utils.SleepUtils;

public class AreaAmtBolt extends BaseRichBolt {
	Logger logger = LoggerFactory.getLogger(AreaAmtBolt.class);
	
	private static final long serialVersionUID = 1L;
	
	private OutputCollector collector;
	private Map<String,Double> map = null;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		map = new HashMap<String,Double>();
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

}
