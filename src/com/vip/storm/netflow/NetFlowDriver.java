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
		// ����TopologyBuilder��ʵ��
		TopologyBuilder builder = new TopologyBuilder();
		// --<1>--����spout��kafka���������� ת��Ϊtuple����
		BrokerHosts hosts = new ZkHosts("192.168.242.101:2181,192.168.242.102:2181,192.168.242.103:2181");
		SpoutConfig sconf = new SpoutConfig(hosts, "netflow", "/netflow",UUID.randomUUID().toString());
		sconf.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout spout = new KafkaSpout(sconf);
		PrintBolt printBolt = new PrintBolt();
		// ����Spout��Kafka��������
		builder.setSpout("KafkaSpout", spout);
		// --<2>--��������
		builder.setBolt("ClearBolt", new ClearBolt()).shuffleGrouping("KafkaSpout");
		// ����PV
		builder.setBolt("PvBolt", new PvBolt()).shuffleGrouping("ClearBolt");
		// ����UV
		builder.setBolt("UvBolt", new UvBolt()).shuffleGrouping("PvBolt");
		// ����VV
		builder.setBolt("VvBolt", new VvBolt()).shuffleGrouping("UvBolt");
		// ����newip
		builder.setBolt("NewIpBolt", new NewIpBolt()).shuffleGrouping("VvBolt");
		// ����newcust
		builder.setBolt("NewCustBolt", new NewCustBolt()).shuffleGrouping("NewIpBolt");
		// --<3>--����HBase(�Ա�ʵʱ�����ѯʹ��)
		builder.setBolt("InsertHBaseBolt", new InsertHBaseBolt()).shuffleGrouping("NewCustBolt");
		// --<4>--����mysql(ʵʱ����Ľ�����DB)
		builder.setBolt("ToMySqlBolt", new ToMySqlBolt()).shuffleGrouping("NewCustBolt");
		// ��ӡ
		builder.setBolt("PrintBolt", printBolt).shuffleGrouping("NewCustBolt");
		// ������д������
		
		//--��������topology
		Config conf = new Config();
		StormTopology topology = builder.createTopology();
		
		
		// ����TopologyBuilderʵ����������br avgtime avgdeep
		// --<1>--���ö�ʱ��ʹ��ÿ��1��Сʱ����һ��
		TopologyBuilder builder2 = new TopologyBuilder();
		// ����ʱ��
		builder2.setBolt("GetTimeBolt", new GetTimeBolt());
		// --<2>--��������
		// ����������
		builder2.setBolt("BrBolt", new BrBolt()).shuffleGrouping("GetTimeBolt");
		// ����Ựƽ��ʱ��
		builder2.setBolt("AvgTimeBolt", new AvgTimeBolt()).shuffleGrouping("BrBolt");
		// ����Ựƽ���������
		builder2.setBolt("AvgDeepBolt", new AvgDeepBolt()).shuffleGrouping("AvgTimeBolt");
		// ��ӡ
		builder2.setBolt("PrintBolt", new PrintBolt()).shuffleGrouping("AvgDeepBolt");
		// --<3>--������д������
		builder2.setBolt("ToMySqlBolt2", new ToMySqlBolt2()).shuffleGrouping("AvgDeepBolt");
		// ����topoloyg2
		Config conf2 = new Config();
		StormTopology topology2 = builder2.createTopology();
		
		
		//--�����˽�����Ⱥȥ����
		//----�ύ������ģ��ļ�Ⱥ�н��в���
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("NetFlowTopology", conf, topology);
		cluster.submitTopology("NetFlowTopology2", conf2, topology2);
		//--10���Ӻ�ֹͣ����
		Thread.sleep(1000 * 1000);
		cluster.killTopology("NetFlowTopology");
		cluster.killTopology("NetFlowTopology2");
		cluster.shutdown();	
	}
}
