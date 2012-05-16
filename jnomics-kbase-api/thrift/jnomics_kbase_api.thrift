namespace java edu.cshl.schatz.jnomics.kbase.thrift.api

struct Authentication{
       1: string username,
       2: string password
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
        JnomicsThriftJobID alignBowtie (1: string inPath, 2: string organism, 3: string outPath, 4: Authentication auth) throws (1: JnomicsThriftException je),
        JnomicsThriftJobID alignBWA (1: string inPath, 2: string organism, 3: string outPath, 4: Authentication auth) throws (1: JnomicsThriftException je),
        JnomicsThriftJobID snpSamtools (1: string inPath, 2: string outPath, 3: Authentication auth) throws (1: JnomicsThriftException je),
	JnomicsThriftJobStatus getJobStatus(1: JnomicsThriftJobID jobID, 3: Authentication auth) throws (1: JnomicsThriftException je),
	list<JnomicsThriftJobStatus> getAllJobs(1: Authentication auth) throws (1: JnomicsThriftException je)
}

service JnomicsData{
        JnomicsThriftHandle create (1: string path, 2: Authentication auth) throws (1: JnomicsThriftException je),
        JnomicsThriftHandle open (1: string path, 2: Authentication auth) throws (1: JnomicsThriftException je),
        void write (1: JnomicsThriftHandle handle, 2: binary data, 3:Authentication auth) throws (1: JnomicsThriftException je),
        binary read (1: JnomicsThriftHandle handle, 2: Authentication auth) throws (1: JnomicsThriftException je),
        void close(1: JnomicsThriftHandle handle, 2: Authentication auth) throws (1: JnomicsThriftException je),
        list<JnomicsThriftFileStatus> listStatus(1: string path, 2:Authentication auth) throws (1: JnomicsThriftException je),
        bool remove(1: string path, 2: bool recursive, 3: Authentication auth) throws (1: JnomicsThriftException je)
}

