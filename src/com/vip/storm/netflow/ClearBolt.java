package com.vip.storm.netflow;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ClearBolt extends BaseRichBolt{
	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// 过滤出所需要的字段(value)
		String str = input.getStringByField("str");
		String attrs [] = str.split("\\|");
		String urlname = attrs[1];
		String uvid = attrs[13];
		String sid = attrs[14].split("_")[0];
		int scount = Integer.parseInt(attrs[14].split("_")[1]);
		long stime = Long.parseLong(attrs[14].split("_")[2]);
		String cip = attrs[15];
		collector.emit(input,new Values(urlname,uvid,sid,scount,stime,cip));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// key
		declarer.declare(new Fields("urlname", "uvid","sid","scount","stime","cip"));
	}

}
