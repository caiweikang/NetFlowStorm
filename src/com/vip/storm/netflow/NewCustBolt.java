package com.vip.storm.netflow;

import java.util.List;
import java.util.Map;

import com.vip.hbase.dao.HBaseDao;
import com.vip.hbase.dao.NetFlowInfo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class NewCustBolt extends BaseRichBolt {
	private OutputCollector collecotr = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collecotr = collector;
	}

	@Override
	public void execute(Tuple input) {
		String uvid = input.getStringByField("uvid");
		List<NetFlowInfo> list = HBaseDao.find("^\\d*_\\d*_\\d*_"+uvid+"_.*$");
		int newcust = list.size() == 0 ? 1 : 0;
		List<Object> values = input.getValues();
		values.add(newcust);
		collecotr.emit(input,values);
		collecotr.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("urlname", "uvid","sid","scount","stime","cip","pv","uv","vv","newip","newcust"));
	}

}
