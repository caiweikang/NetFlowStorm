package com.vip.storm.netflow;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 *      �����Լ�дһ����ʱ�������ݵ�spout
 * @author Administrator
 *
 */
public class TimeSpout extends BaseRichSpout{
	private SpoutOutputCollector collector = null;
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	long begintime=System.currentTimeMillis();
	@Override
	public void nextTuple() {
		long nowtime = System.currentTimeMillis();
		if(nowtime - begintime >= 1000 * 3600){
			Object nowtiem;
			collector.emit(new Values(nowtime));
			begintime = nowtime;
		}else{
			return;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("signal"));
	}

}
