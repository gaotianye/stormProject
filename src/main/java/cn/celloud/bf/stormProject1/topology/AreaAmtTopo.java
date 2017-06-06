package cn.celloud.bf.stormProject1.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import cn.celloud.bf.stormProject1.consumer.Constant;
import cn.celloud.bf.stormProject1.spout.OrderBaseSpout;

public class AreaAmtTopo {
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		// 5个线程，每个线程里面使用1个线程消费kafka中的数据
		topologyBuilder.setSpout("spout", new OrderBaseSpout(Constant.TOPIC),5);
		Config config = new Config();
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
