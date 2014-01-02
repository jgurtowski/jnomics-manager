//for documentation view jnomics_thrift_api.hpp

namespace java edu.cshl.schatz.jnomics.manager.api

struct Authentication{
       1: string username,
       2: string password,
       3: string token
}

struct JnomicsThriftJobID{
       1: string job_id
}

struct JnomicsThriftHandle{
       1: string uuid
}

struct JnomicsThriftFileStatus{
       1: bool isDir,
       2: string path,
       3: string owner,
       4: string group,
       5: string permission,
       6: i16 replication,
       7: i64 mod_time,
       8: i64 block_size,
       9: i64 length
}

struct JnomicsThriftJobStatus{
       1: string job_id,
       2: string username,
       3: string failure_info,
       4: bool complete,
       5: i32 running_state,
       6: i64 start_time,
       7: string priority,       
       8: double mapProgress,
       9: double reduceProgress
}


exception JnomicsThriftException{
       1: string msg
}


service JnomicsCompute{
        
        JnomicsThriftJobID alignBowtie (1: string inPath, 2: string organism, 3: string outPath, 4: string opts, 5: Authentication auth) throws (1: JnomicsThriftException je),
 
        JnomicsThriftJobID alignBWA (1: string inPath, 2: string organism, 3: string outPath, 4: string alignOpts, 5: string sampeOpts, 6: Authentication auth) throws (1: JnomicsThriftException je),

		JnomicsThriftJobID alignTophat(1: string ref_genome, 2: string inPath ,3: string gtffile, 4: string outPath, 5: string alignOpts,6: string workingdir, 7: Authentication auth)throws (1: JnomicsThriftException je),
		
		JnomicsThriftJobID callCufflinks( 1: string inPath ,2: string outpath, 3: string ref_gtf, 4: string alignOpts,5: string workingdir, 6: Authentication auth)throws (1: JnomicsThriftException je),
		
		JnomicsThriftJobID callCuffmerge( 1: string inPath ,2: string ref_genome,3: string outpath, 4: string alignOpts , 5: string gtffile ,6: string workingdir, 7: Authentication auth)throws (1: JnomicsThriftException je),
		
		JnomicsThriftJobID callCuffdiff( 1: string inPath , 2: string outpath, 3: string ref_genome,4: string alignOpts, 5: string condn_labels, 6: string merged_gtf, 7: string workingdir, 8: Authentication auth)throws (1: JnomicsThriftException je),
	
		JnomicsThriftJobID callCuffcompare( 1: string inPath ,2: string outpath, 3: string alignOpts , 4: string gtffile ,5: string workingdir, 6: Authentication auth)throws (1: JnomicsThriftException je),
		
		JnomicsThriftJobID ShockRead (1: string nodeId , 2: string inPath,3: Authentication auth) throws (1: JnomicsThriftException je),
		
		JnomicsThriftJobID ShockWrite (1: string filename,2: string hdfsPath,3: Authentication auth) throws (1: JnomicsThriftException je),
		
		JnomicsThriftJobID workspaceUpload(1: string filename, 2: string kb_id, 3: string genome_id, 4: string onto_term_id, 5: string onto_term_def, 6: string onto_term_name,7: string seq_type,8: string reference,9: Authentication auth) throws (1: JnomicsThriftException je),
		
		JnomicsThriftJobID ShockBatchWrite (1: list<string> inPath , 2: string outPath,3: Authentication auth) throws (1: JnomicsThriftException je),

        JnomicsThriftJobID snpSamtools (1: string inPath, 2: string organism, 3: string outPath, 4: Authentication auth) throws (1: JnomicsThriftException je),

       	JnomicsThriftJobStatus getJobStatus(1: JnomicsThriftJobID jobID, 3: Authentication auth) throws (1: JnomicsThriftException je),
       	
		string getGridJobStatus(1: JnomicsThriftJobID jobID, 2: Authentication auth) throws (1: JnomicsThriftException je) ,
	
	list<JnomicsThriftJobStatus> getAllJobs(1: Authentication auth) throws (1: JnomicsThriftException je),
        

        bool mergeVCF(1: string inDir, 2: string inAlignments, 3: string outVCF, 4: Authentication auth) throws (1: JnomicsThriftException je),
       
        bool mergeCovariate(1: string inDir, 2: string outCov, 3: Authentication auth) throws (1: JnomicsThriftException je),
        JnomicsThriftJobID gatkRealign (1: string inPath, 2: string organism, 3: string outPath, 4: Authentication auth) throws (1: JnomicsThriftException je),
        JnomicsThriftJobID gatkCallVariants (1: string inPath, 2: string organism, 3: string outPath, 4: Authentication auth) throws (1:JnomicsThriftException je),
        JnomicsThriftJobID gatkCountCovariates (1: string inPath, 2: string organism, 3: string vcfMask, 4: string outPath, 5: Authentication auth) throws (1:JnomicsThriftException je),
        JnomicsThriftJobID gatkRecalibrate (1: string inPath, 2: string organism, 3: string recalFile, 4: string outPath, 5: Authentication auth) throws (1: JnomicsThriftException je),
        JnomicsThriftJobID runSNPPipeline(1: string inPath, 2: string organism, 3: string outPath, 4: Authentication auth) throws (1: JnomicsThriftException je)

        JnomicsThriftJobID pairReads(1: string file1, 2: string file2, 3: string outFile, 4: Authentication auth) throws (1: JnomicsThriftException je),
        JnomicsThriftJobID singleReads(1: string file, 2: string outFile, 3: Authentication auth) throws (1: JnomicsThriftException je),

}


service JnomicsData{	
        JnomicsThriftHandle create (1: string path, 2: Authentication auth) throws (1: JnomicsThriftException je),
        JnomicsThriftHandle open (1: string path, 2: Authentication auth) throws (1: JnomicsThriftException je),
        void write (1: JnomicsThriftHandle handle, 2: binary data, 3:Authentication auth) throws (1: JnomicsThriftException je),
        binary read (1: JnomicsThriftHandle handle, 2: Authentication auth) throws (1: JnomicsThriftException je),
        void close(1: JnomicsThriftHandle handle, 2: Authentication auth) throws (1: JnomicsThriftException je),
        list<JnomicsThriftFileStatus> listStatus(1: string path, 2:Authentication auth) throws (1: JnomicsThriftException je),
        bool checkFileStatus(1: string path, 2:Authentication auth) throws (1: JnomicsThriftException je),
		list<string> listShockStatus(1: string path, 2:Authentication auth) throws (1: JnomicsThriftException je),
        bool remove(1: string path, 2: bool recursive, 3: Authentication auth) throws (1: JnomicsThriftException je),
        bool mkdir(1: string path, 2: Authentication auth) throws (1: JnomicsThriftException je),
        bool mv(1: string path, 2: string dest, 3:Authentication auth) throws (1: JnomicsThriftException je),
        list<string> listGenomes(1:Authentication auth) throws (1: JnomicsThriftException je)

}

