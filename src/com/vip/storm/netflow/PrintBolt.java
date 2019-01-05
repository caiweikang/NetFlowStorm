package com.vip.storm.netflow;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PrintBolt extends BaseRichBolt{
	private OutputCollector collector = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Fields fields = input.getFields();
		Iterator<String> it = fields.iterator();
		StringBuffer buffer = new StringBuffer();
		buffer.append("----");
		while(it.hasNext()){
			String key = it.next();
			Object value = input.getValueByField(key);
			buffer.append("-"+key+":"+value+"-");
		}
		buffer.append("----");
		
		System.out.println(buffer.toString());
		
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
}
