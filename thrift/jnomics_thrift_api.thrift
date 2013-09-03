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

		JnomicsThriftJobID ShockBatchWrite (1: list<string> inPath , 2: string outPath,3: Authentication auth) throws (1: JnomicsThriftException je),

        JnomicsThriftJobID snpSamtools (1: string inPath, 2: string organism, 3: string outPath, 4: Authentication auth) throws (1: JnomicsThriftException je),


       	JnomicsThriftJobStatus getJobStatus(1: JnomicsThriftJobID jobID, 3: Authentication auth) throws (1: JnomicsThriftException je),

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
        bool ShockRead (1: string nodeId , 2: string inPath,3: Authentication auth) throws (1: JnomicsThriftException je),
        bool ShockWrite (1: string filename , 2: string inPath,3: Authentication auth) throws (1: JnomicsThriftException je),
        void close(1: JnomicsThriftHandle handle, 2: Authentication auth) throws (1: JnomicsThriftException je),
        list<JnomicsThriftFileStatus> listStatus(1: string path, 2:Authentication auth) throws (1: JnomicsThriftException je),
	list<string> listShockStatus(1: string path, 2:Authentication auth) throws (1: JnomicsThriftException je),
        bool remove(1: string path, 2: bool recursive, 3: Authentication auth) throws (1: JnomicsThriftException je),
        bool mkdir(1: string path, 2: Authentication auth) throws (1: JnomicsThriftException je),
        bool mv(1: string path, 2: string dest, 3:Authentication auth) throws (1: JnomicsThriftException je),
        list<string> listGenomes(1:Authentication auth) throws (1: JnomicsThriftException je)

}

