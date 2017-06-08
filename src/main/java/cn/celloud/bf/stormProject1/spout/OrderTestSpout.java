package cn.celloud.bf.stormProject1.spout;

import java.util.Date;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cn.celloud.bf.utils.DateUtils;

public class OrderTestSpout extends BaseRichSpout {
	Logger logger = LoggerFactory.getLogger(OrderTestSpout.class);
	
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private Integer taskId;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		taskId = context.getThisTaskId();
	}
	
	int i = 1;
	public void nextTuple() {
		Random random = new Random();
		String[] order_amt = {"10.10","20.10","50.20","70.50","15.50",
				"25.50","30.50","55.10","80.50","20.60",
				"89.50","90.80"};
		String[] area_id = {"1","2","3","4","5"};
		String message = (i++)+"\t"+order_amt[random.nextInt(12)]+"\t"+
				DateUtils.formatTime(new Date())+"\t"+area_id[random.nextInt(5)];
//		logger.info("message:{}",message);
		collector.emit(new Values(message));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("order"));
	}

}
