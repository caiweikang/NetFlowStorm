package com.vip.storm.netflow;

import java.util.Map;

import com.vip.hbase.dao.HBaseDao;
import com.vip.hbase.dao.NetFlowInfo;
import com.vip.utils.DateUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class InsertHBaseBolt extends BaseRichBolt {
	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// 将这条数据存储到HBase中以供后续使用
		NetFlowInfo nfi = new NetFlowInfo();
		nfi.setTime(DateUtils.format(input.getLongByField("stime")));
		nfi.setUrlname(input.getStringByField("urlname"));
		nfi.setUvid(input.getStringByField("uvid"));
		nfi.setSid(input.getStringByField("sid"));
		nfi.setScount(input.getIntegerByField("scount")+"");
		nfi.setStime(input.getLongByField("stime")+"");
		nfi.setCip(input.getStringByField("cip"));
		HBaseDao.add(nfi);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
