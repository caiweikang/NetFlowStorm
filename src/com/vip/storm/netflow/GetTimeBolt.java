package com.vip.storm.netflow;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.vip.hbase.dao.HBaseDao;
import com.vip.hbase.dao.NetFlowInfo;
import com.vip.utils.DateUtils;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GetTimeBolt extends BaseRichBolt{

	/**
	 *    ��д�˷�����������ʱ�䣬ÿ�����ʱ��ͻ�ִ��һ��execute�����Ϳ�ʼ�ͽ�ֹʱ�䣬
	 *    �˺������boltҲ��ÿ�����ʱ��ִ��һ��
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3600);
		return conf;
	}
	
	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Calendar calendar = Calendar.getInstance();
		calendar.clear(Calendar.MINUTE);
		calendar.clear(Calendar.SECOND);
		calendar.clear(Calendar.MILLISECOND);
		
		calendar.add(Calendar.HOUR, 1);
		long endTime = calendar.getTimeInMillis();
		calendar.add(Calendar.HOUR, -1);
		long beginTime = calendar.getTimeInMillis();
		
		collector.emit(input,new Values(beginTime,endTime));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("beginTime","endTime"));
	}

}
