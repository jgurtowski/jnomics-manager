
namespace java edu.cshl.schatz.jnomics.kbase.thrift.api

struct Authentication{
       1: string username,
       2: string password
}

struct JnomicsJobID{
       1: string job_id
}

struct JnomicsThriftHandle{
       1: string uuid
}

struct JnomicsFileStatus{
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


exception JnomicsDataException{
       1: string msg
}

service JnomicsCompute{
        JnomicsJobID alignBowtie (1: string inPath, 2: string organism, 3: string outPath, 4: Authentication auth),
        JnomicsJobID alignBWA (1: string inPath, 2: string organism, 3: string outPath, 4: Authentication auth),
        JnomicsJobID snpSamtools (1: string inPath, 2: string outPath, 3: Authentication auth)
}

service JnomicsData{
        JnomicsThriftHandle create (1: string path, 2: Authentication auth) throws (1: JnomicsDataException je),
        JnomicsThriftHandle open (1: string path, 2: Authentication auth) throws (1: JnomicsDataException je),
        void write (1: JnomicsThriftHandle handle, 2: binary data, 3:Authentication auth) throws (1: JnomicsDataException je),
        binary read (1: JnomicsThriftHandle handle, 2: Authentication auth) throws (1: JnomicsDataException je),
        void close(1: JnomicsThriftHandle handle, 2: Authentication auth) throws (1: JnomicsDataException je),
        list<JnomicsFileStatus> listStatus(1: string path, 2:Authentication auth) throws (1: JnomicsDataException je),
        bool remove(1: string path, 2: bool recursive, 3: Authentication auth) throws (1: JnomicsDataException je)
}

