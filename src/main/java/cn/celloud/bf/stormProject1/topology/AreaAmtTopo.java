package cn.celloud.bf.stormProject1.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cn.celloud.bf.stormProject1.bolt.AreaAmtBolt;
import cn.celloud.bf.stormProject1.bolt.AreaFilterBolt;
import cn.celloud.bf.stormProject1.bolt.AreaResBolt;
import cn.celloud.bf.stormProject1.constant.Constant;
import cn.celloud.bf.stormProject1.spout.OrderBaseSpout;
import cn.celloud.bf.stormProject1.spout.OrderTestSpout;

public class AreaAmtTopo {
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		// 5个线程，每个线程里面使用1个线程消费kafka中的数据
//		topologyBuilder.setSpout("spout", new OrderBaseSpout(Constant.ORDER_TOPIC),5);
		topologyBuilder.setSpout("spout", new OrderTestSpout(),1);
		topologyBuilder.setBolt("filter", new AreaFilterBolt(), 1).shuffleGrouping("spout");
		topologyBuilder.setBolt("areabolt", new AreaAmtBolt(), 1).fieldsGrouping("filter", new Fields("area_id"));
		topologyBuilder.setBolt("resbolt", new AreaResBolt(), 1).shuffleGrouping("areabolt");
		
		Config config = new Config();
		config.setDebug(false);
		//如果是 storm jar xx.jar xxxxx
		String simpleName = AreaAmtTopo.class.getSimpleName();
		if(args.length>0){
			try {
				StormSubmitter.submitTopology(simpleName,config,topologyBuilder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		//如果没有带有参数，则本地测试
		}else{
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("localTopology", config, topologyBuilder.createTopology());
		}
	}
}
