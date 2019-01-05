package com.vip.storm.netflow;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.vip.hbase.dao.HBaseDao;
import com.vip.hbase.dao.NetFlowInfo;
import com.vip.utils.DateUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class AvgDeepBolt extends BaseRichBolt{
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
		//��ѯ�˷�Χ�ڵ�����
		List<NetFlowInfo> list = HBaseDao.find2(begin, end);
		
		//ͳ�ƻỰȥ��ҳ��
		Map<String,Set<String>> map = new HashMap<>();
		for(NetFlowInfo nfi : list){
			if(map.containsKey(nfi.getSid())){
				map.get(nfi.getSid()).add(nfi.getUrlname());
			}else{
				Set<String> set = new HashSet<>();
				set.add(nfi.getUrlname());
				map.put(nfi.getSid(), set);
			}
		}
		
		//����ƽ���������
		int allDeep = 0;
		int sCount = map.size();
		for(Map.Entry<String, Set<String>> entry : map.entrySet()){
			Set<String> set = entry.getValue();
			allDeep += set.size();
		}
		double avgDeep = Math.round(allDeep*1000.0/sCount)/1000.0;
		
		//�������
		List<Object> values = input.getValues();
		values.add(avgDeep);
		collector.emit(input,values);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("beginTime","endTime","br","avgtime","avgdeep"));		
	}
	
}
