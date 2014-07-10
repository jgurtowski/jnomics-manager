package edu.cshl.schatz.jnomics.manager.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import us.kbase.idserver.IDServerAPI;

public class IDServer{
    
    public static String registerId(String prefix, String key) throws Exception{
    	List<String> keys = null;
    	Map<String, String> id = null;
    	IDServerAPI idserver;
    	try{
    		keys = new ArrayList<String>();
    		id = new HashMap<String, String>();
    		keys.add(key);
    		idserver = new IDServerAPI("http://kbase.us/services/idserver");
    		id = idserver.register_ids(prefix, null, keys);
//          int id = idserver.allocate_id_range("kb|sample", 1);
//          System.out.println("New Id: " + id);
        }catch(Exception e ){
        	e.printStackTrace();
        }
        return id.get(keys.get(0));
    }
}
