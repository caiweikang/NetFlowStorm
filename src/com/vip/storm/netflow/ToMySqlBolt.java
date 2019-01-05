package com.vip.storm.netflow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.vip.utils.DateUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
	create table netFlowInfo1(
		reportTime date primary key,
		pv int,
		uv int,
		vv int,
		newip int,
		newcust int
	);
 */
public class ToMySqlBolt extends BaseRichBolt {
	private OutputCollector collector = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	//"urlname", "uvid","sid","scount","stime","cip","pv","uv","vv","newip","newcust")
	@Override
	public void execute(Tuple input) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String dateStr = DateUtils.format(input.getLongByField("stime"));
			int pv = input.getIntegerByField("pv");
			int uv = input.getIntegerByField("uv");
			int vv = input.getIntegerByField("vv");
			int newip = input.getIntegerByField("newip");
			int newcust = input.getIntegerByField("newcust");
			
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://192.168.242.101:3306/netflowdb","root","123456");
			conn.setAutoCommit(false);
			
			String sql = "select * from netFlowInfo1 where reportTime = ?";
			ps = conn.prepareStatement(sql);
			ps.setString(1,dateStr);
			rs = ps.executeQuery();
			
			if(rs.next()){
				// 之前有记录 - 修改
				String sqlx = "update netFlowInfo1 set pv = ?,uv =? ,vv=?,newip=?,newcust = ? where reportTime = ?";
				ps = conn.prepareStatement(sqlx);
				ps.setInt(1, rs.getInt("pv")+pv); // 在原来的基础上加1
				ps.setInt(2, rs.getInt("uv")+uv);
				ps.setInt(3, rs.getInt("vv")+vv);
				ps.setInt(4, rs.getInt("newip")+newip);
				ps.setInt(5, rs.getInt("newcust")+newcust);
				ps.setString(6, dateStr);
				ps.executeUpdate();
			} else{
				// 之前没有记录 - 新增
				String sqlx = "insert into netFlowInfo1 values (?,?,?,?,?,?)";
				ps = conn.prepareStatement(sqlx);
				ps.setString(1, dateStr);
				ps.setInt(2, pv);
				ps.setInt(3, uv);
				ps.setInt(4, vv);
				ps.setInt(5, newip);
				ps.setInt(6, newcust);
				ps.executeUpdate();
			}
			collector.ack(input);
			conn.commit();
		} catch (Exception e) {
			if(conn!=null){
				try {
					conn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
					throw new RuntimeException(e1);
				}
			}
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if(rs!=null){
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}finally {
					rs = null;
				}
			}
			if(ps!=null){
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}finally {
					ps = null;
				}
			}
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}finally {
					conn = null;
				}
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
