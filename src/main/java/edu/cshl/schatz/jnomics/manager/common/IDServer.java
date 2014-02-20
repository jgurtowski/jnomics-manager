package edu.cshl.schatz.jnomics.manager.common;

import us.kbase.idserver.IDServerAPI;

public class IDServer{
    
    public static int registerId() throws Exception{
        
        IDServerAPI idserver = new IDServerAPI("http://kbase.us/services/idserver");
        int id = idserver.allocate_id_range("kb|sample_test", 1);
        System.out.println("New Id: " + id);
        return id;
    }
}
