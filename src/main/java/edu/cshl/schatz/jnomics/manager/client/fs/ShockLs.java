package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftFileStatus;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class ShockLs extends FSBase {

	@Flag(shortForm = "-h", longForm = "--help")
	public boolean help;

	@Override
	public void handle(List<String> remainingArgs, Properties properties) throws Exception {
		super.handle(remainingArgs,properties);

		if(help){
			System.out.println("-shock_ls");
			return;
		}

		String dest = ".";
		if(remainingArgs.size() >= 1){
			dest = remainingArgs.get(0);
		}

		List<String> stats = client.listShockStatus(dest,auth);

		System.out.println("Found "+ stats.size() + " items");
		for(String status: stats){
			System.out.println(status);
		}
	}

}
