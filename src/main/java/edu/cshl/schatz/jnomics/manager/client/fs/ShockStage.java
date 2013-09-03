package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftHandle;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.client.ann.Parameter;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class ShockStage extends FSBase{

	@Flag(shortForm = "-h", longForm = "--help")
	public boolean help;

	@Parameter(shortForm = "-shock_id", longForm = "--shockID", description = "Shock File ID")
	public String shock_id;

	@Parameter(shortForm = "-dest", longForm = "--clusterDest", description = "Destination on the Cluster")
	public String dest;


	@Override
	public void handle(List<String> remainingArgs, Properties properties) throws Exception {
		super.handle(remainingArgs, properties);

		if(help || shock_id == null || dest == null){
			System.out.println("stage_shock -shock_id=<id> -dest=<dest>");
		}else{
			client.ShockRead(shock_id, dest, auth);
		}
	}
}
