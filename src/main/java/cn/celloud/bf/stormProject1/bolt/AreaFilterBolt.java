package cn.celloud.bf.stormProject1.bolt;

import java.text.DecimalFormat;
import java.text.ParseException;
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
import cn.celloud.bf.stormProject1.spout.OrderTestSpout;
/**
 * order_id,order_amt,create_time,area_id
 * 时间转换成：yyyy-MM-dd
 * @author Administrator
 *
 */
public class AreaFilterBolt extends BaseRichBolt {
	Logger logger = LoggerFactory.getLogger(AreaFilterBolt.class);
	
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String order = input.getStringByField("order");
		if(order!=null){
			String[] split = order.split("\t");
			Double order_amt = Double.parseDouble(split[1]);
			String create_time = split[2].split(" ")[0];
			Integer area_id = Integer.parseInt(split[3]);
//			logger.info("order_amt:{},create_time:{},area_id:{}",order_amt,create_time,area_id);
			collector.emit(new Values(order_amt,create_time,area_id));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("order_amt","create_time","area_id"));
	}

}
