package com.elex.hive;

	import org.apache.hadoop.hive.ql.exec.UDF;
	   public  class UDFTest  extends UDF{   
	      public String evaluate(){   
	           return "hello world!";   
	     }   
	      public String evaluate(String str){   
	           return "hello world: " + str;   
	     }   
	}

