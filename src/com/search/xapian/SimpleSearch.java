package com.search.xapian;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.io.File;
import java.nio.ByteBuffer;

import org.xapian.*;

public class SimpleSearch {

    public static void main(String[] args) throws Exception {
    	File f = new File("K://IIT Life//Lecture Notes//SEM 1 - DIC AOS DPA//DIC//xaaaaaaaadt");
    	long x = f.length() - 640097;
    	System.out.println("File size in Bytes" + x); 
    	// conversion from long to byte array
    	System.out.println("File size in byte array" + longToBytes(x));
    	
    	// conversion back from byte array to long
    	System.out.println("File size in byte array" + bytesToLong(longToBytes(x)));
    	
        String sampleString = "6 2020878165 876 327 BmhM8sr 2020878165 2Nr 1 44768 41120 -rw-r--r-- 1419249645 1419249646 4194304 1419249646 /fboK6WL/AqHS1Nqfjm/8kzpspRysZ/fboKjatVtSR/BmhM8sr/sTQ/yJerMdz/LZHZ/ARHNGQnzhClN/ARHNGQnzWTWs/gQnIfKz9XF6vJloSScuPFPikZGGKJw7mGUsSr";
        String[] stringArray = sampleString.split(" ");        
        //3,4,8,9,10,11,12,14,15
        String indexLine = stringArray[3] + " " + stringArray[4] + " " + stringArray[8] + " " + stringArray[9] + " " + stringArray[10] + " " + stringArray[11] + " " + stringArray[12] + " " + stringArray[14]+ " " + stringArray[15];
        System.out.println(indexLine);
        
        printUsage();
}		
    
    private static void printUsage() {
    	int usage = 0;
    	  OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
    	  for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
    	    method.setAccessible(true);
    	    if (method.getName().startsWith("get")
    	        && Modifier.isPublic(method.getModifiers())) {
    	            Object value;
    	        try {
    	            value = method.invoke(operatingSystemMXBean);
    	        } catch (Exception e) {
    	            value = e;
    	        } 
    	        System.out.println("MEMORY USAGE: " + method.getName() + " = " + value);
    	    }
    	  } 
    	}
    
    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip 
        return buffer.getLong();
    }
    
}