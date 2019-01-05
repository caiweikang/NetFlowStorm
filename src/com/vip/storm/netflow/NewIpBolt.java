package com.vip.storm.netflow;

import java.util.List;
import java.util.Map;

import com.vip.hbase.dao.HBaseDao;
import com.vip.hbase.dao.NetFlowInfo;
import com.vip.utils.DateUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class NewIpBolt extends BaseRichBolt {
	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		long time = input.getLongByField("stime");
		String dateStr = DateUtils.format(time);
		String cip = input.getStringByField("cip");
		List<NetFlowInfo> list = HBaseDao.find("^"+dateStr+"_\\d*_\\d*_\\d*_"+cip+"_\\d*$");
		int newip = list.size() == 0 ? 1 : 0;
		
		List<Object> values = input.getValues();
		values.add(newip);
		collector.emit(input,values);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("urlname", "uvid","sid","scount","stime","cip","pv","uv","vv","newip"));
	}

}
