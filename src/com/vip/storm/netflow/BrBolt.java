package com.vip.storm.netflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.vip.hbase.dao.HBaseDao;
import com.vip.hbase.dao.NetFlowInfo;
import com.vip.utils.DateUtils;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BrBolt extends BaseRichBolt {

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		long beginTime = input.getLongByField("beginTime");
		long endTime = input.getLongByField("endTime");
		
		byte [] begin = (DateUtils.format(beginTime)+"_"+beginTime).getBytes();
		byte [] end = (DateUtils.format(endTime)+"_"+endTime).getBytes();
		//查询这个范围内的数据
		List<NetFlowInfo> list = HBaseDao.find2(begin,end);
		
		//遍历这个小时内的所有的访问记录，统计有多少个会话
		Map<String,Integer> map = new HashMap<>();
		for(NetFlowInfo nfi : list){
			String sid = nfi.getSid();
			map.put(sid, map.containsKey(sid) ? map.get(sid) + 1 : 1);
		}
		
		//计算跳出率
		//--会话总数
		int scount = map.size();
		//--跳出的会话总数
		int brcount = 0;
		for(Map.Entry<String, Integer>entry : map.entrySet()){
			brcount = entry.getValue() == 1 ? brcount+1 : brcount;
		}
		//--计算跳出率
		double br = Math.round(brcount * 1.0/scount * 1000) / 1000.0;
		
		//--输出数据
		List<Object> values = input.getValues();
		values.add(br);
		collector.emit(input,values);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("beginTime","endTime","br"));
	}

}
