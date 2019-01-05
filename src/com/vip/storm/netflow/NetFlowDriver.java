package com.vip.storm.netflow;

import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class NetFlowDriver {
	public static void main(String[] args) throws Exception {
		// 创建TopologyBuilder类实例
		TopologyBuilder builder = new TopologyBuilder();
		// --<1>--创建spout从kafka中消费数据 转换为tuple发送
		BrokerHosts hosts = new ZkHosts("192.168.242.101:2181,192.168.242.102:2181,192.168.242.103:2181");
		SpoutConfig sconf = new SpoutConfig(hosts, "netflow", "/netflow",UUID.randomUUID().toString());
		sconf.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout spout = new KafkaSpout(sconf);
		PrintBolt printBolt = new PrintBolt();
		// 设置Spout从Kafka消费数据
		builder.setSpout("KafkaSpout", spout);
		// --<2>--整理数据
		builder.setBolt("ClearBolt", new ClearBolt()).shuffleGrouping("KafkaSpout");
		// 计算PV
		builder.setBolt("PvBolt", new PvBolt()).shuffleGrouping("ClearBolt");
		// 计算UV
		builder.setBolt("UvBolt", new UvBolt()).shuffleGrouping("PvBolt");
		// 计算VV
		builder.setBolt("VvBolt", new VvBolt()).shuffleGrouping("UvBolt");
		// 计算newip
		builder.setBolt("NewIpBolt", new NewIpBolt()).shuffleGrouping("VvBolt");
		// 计算newcust
		builder.setBolt("NewCustBolt", new NewCustBolt()).shuffleGrouping("NewIpBolt");
		// --<3>--更新HBase(以备实时处理查询使用)
		builder.setBolt("InsertHBaseBolt", new InsertHBaseBolt()).shuffleGrouping("NewCustBolt");
		// --<4>--更新mysql(实时处理的结果落地DB)
		builder.setBolt("ToMySqlBolt", new ToMySqlBolt()).shuffleGrouping("NewCustBolt");
		// 打印
		builder.setBolt("PrintBolt", printBolt).shuffleGrouping("NewCustBolt");
		// 将数据写入数据
		
		//--创建拓扑topology
		Config conf = new Config();
		StormTopology topology = builder.createTopology();
		
		
		// 创建TopologyBuilder实例，来计算br avgtime avgdeep
		// --<1>--设置定时器使其每隔1个小时计算一次
		TopologyBuilder builder2 = new TopologyBuilder();
		// 生成时间
		builder2.setBolt("GetTimeBolt", new GetTimeBolt());
		// --<2>--整理数据
		// 计算跳出率
		builder2.setBolt("BrBolt", new BrBolt()).shuffleGrouping("GetTimeBolt");
		// 计算会话平均时长
		builder2.setBolt("AvgTimeBolt", new AvgTimeBolt()).shuffleGrouping("BrBolt");
		// 计算会话平均访问深度
		builder2.setBolt("AvgDeepBolt", new AvgDeepBolt()).shuffleGrouping("AvgTimeBolt");
		// 打印
		builder2.setBolt("PrintBolt", new PrintBolt()).shuffleGrouping("AvgDeepBolt");
		// --<3>--将数据写入数据
		builder2.setBolt("ToMySqlBolt2", new ToMySqlBolt2()).shuffleGrouping("AvgDeepBolt");
		// 创建topoloyg2
		Config conf2 = new Config();
		StormTopology topology2 = builder2.createTopology();
		
		
		//--将拓扑交给集群去运行
		//----提交到本地模拟的集群中进行测试
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("NetFlowTopology", conf, topology);
		cluster.submitTopology("NetFlowTopology2", conf2, topology2);
		//--10秒钟后停止程序
		Thread.sleep(1000 * 1000);
		cluster.killTopology("NetFlowTopology");
		cluster.killTopology("NetFlowTopology2");
		cluster.shutdown();	
	}
}
