package com.vip.storm.netflow;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PvBolt extends BaseRichBolt {
	private OutputCollector collector = null; 
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// ��ȡ���е��ֶΣ����ڴ˻����ϼ���pv�ֶΣ�ÿһ��tuple����1
		List<Object> values = input.getValues();
		values.add(1);
		// ê��
		collector.emit(input,values);
		// ����tuple���߸�spout���ͳɹ�����
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("urlname", "uvid","sid","scount","stime","cip","pv"));
	}

}
