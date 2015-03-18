package com.elex.hive;

import org.apache.hadoop.hive.ql.exec.NumericUDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.LongWritable;

public class AVGUDAF extends NumericUDAF {
	public static class Result{
		long nCount;
		long nUser;
		public Result(){
			this.nCount = 0;
			this.nUser = 0;
		}
	}
	public static class Evaluator implements UDAFEvaluator {
		Result rs;
		public void init() {
			// TODO Auto-generated method stub
			rs = null;
		}
		public boolean iterate(LongWritable num){
			if(rs == null){
				rs = new Result();
			}
			if(num != null){
				rs.nCount += num.get();
				rs.nUser++;
			}
			return true;
		}
		public Result terminatePartial(){
			return rs;
		}
		public boolean merge(Result other){
			if(other == null){
				return true;
			} 
			if(rs == null){
				rs = new Result();
				rs.nCount = other.nCount;
				rs.nUser = other.nUser;
			}
			else{
				rs.nCount += other.nCount;
				rs.nUser += other.nUser;
			}
			return true;
		}
		public LongWritable terminate(){
			return rs.nUser == 0? null: new LongWritable(rs.nCount/rs.nUser);
		}
	}
}
