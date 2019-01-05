package com.vip.storm.netflow;

import java.util.ArrayList;
import java.util.HashMap;
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

public class AvgTimeBolt extends BaseRichBolt {
	private OutputCollector collector = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		long beginTime = input.getLongByField("beginTime");
		long endTime = input.getLongByField("endTime");
		
		//查询数据这一个小时内的
		byte begin [] = (DateUtils.format(beginTime)+"_"+beginTime).getBytes();
		byte end [] = (DateUtils.format(endTime)+"_"+endTime).getBytes();
		List<NetFlowInfo> list = HBaseDao.find2(begin, end);
		
		//将查到的数据按照sid分开存放
		Map<String,List<NetFlowInfo>> map = new HashMap<>();
		for(NetFlowInfo nfi : list){
			if(map.containsKey(nfi.getSid())){
				map.get(nfi.getSid()).add(nfi);
			}else{
				List<NetFlowInfo> nfiList = new ArrayList<>();
				nfiList.add(nfi);
				map.put(nfi.getSid(), nfiList);
			}
		}
		
		//遍历map，计算每个会话的时长
		int allSsCount = map.size();
		long allSsTime = 0;
		for(Map.Entry<String, List<NetFlowInfo>> entry : map.entrySet()){
			long ssStartTime = Long.MAX_VALUE;
			long ssEndTime = Long.MIN_VALUE;
			List<NetFlowInfo> nfis = entry.getValue();
			for(NetFlowInfo nfi : nfis){
				long stime = Long.parseLong(nfi.getStime());
				if(stime < ssStartTime){
					ssStartTime = stime;
				}
				if(stime > ssEndTime){
					ssEndTime = stime;
				}
			}
			
			allSsTime += (ssEndTime - ssStartTime);
		}
		
		long avgTime = allSsCount == 0 ? 0 : allSsTime/allSsCount;
		
		//输出数据
		List<Object> values = input.getValues();
		values.add(avgTime);
		collector.emit(input,values);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("beginTime","endTime","br","avgtime"));
	}

}
