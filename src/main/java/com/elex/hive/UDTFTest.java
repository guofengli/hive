package com.elex.hive;

import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class UDTFTest extends GenericUDTF{

	
	
	
	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		// TODO Auto-generated method stub
		ArrayList<String> fields = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fields.add("key");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fields.add("value");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		return ObjectInspectorFactory.getStandardStructObjectInspector(fields, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		// TODO Auto-generated method stub
		String tmp = args[0].toString();
		String []datas = tmp.split(":");
		forward(datas);	
	}
	
	
	

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
		
	}

}
