/**\file Thrift API for Jnomics
*
*/

/**\class Authentication
*\brief Authentication Container.
*/
struct Authentication{
  string username; /**< user's username */
  string password;/**< user's password */
  string token; /**< globus online token */
};


/**\class JnomicsThriftJobID
*\brief Job ID Container
*/
struct JnomicsThriftJobID{
  string job_id; /**< hadoop job id */
};

/**\class JnomicsThriftHandle 
*\brief Container containing a hanlde to open file in hdfs
*/
struct JnomicsThriftHandle{
  string uuid; /**< hdfs filehandle uuid */
};

/**\class JnomicsThriftFileStatus
*\brief File Status Container. Everything you need to know about files in hdfs
*/
struct JnomicsThriftFileStatus{
  bool isDir; /**< is directory? */
  string path;/**< path in hdfs */
  string owner;/**< the owner acl */
  string group;/**< the group acl */
  string permission;/**< permissions acl */
  i16 replication;/**< hdfs replication */
  i64 mod_time;/**< last modification time */
  i64 block_size;/**< block size in hdfs */
  i64 length;/**< size of file */
};

/**\class JnomicsThriftJobStatus 
*\brief Container items related to a running Job's status
*/
struct JnomicsThriftJobStatus{
  string job_id;/**<  job id */
  string username;/**< user who ran the job */
  string failure_info;/**< failure information */
  bool complete;/**< is the job complete */
  i32 running_state;/**< the current running state integer */
  i64 start_time;/**< start time of the job */
  string priority;/**< job's priority on the cluster */
  double mapProgress;/**< the progress in map tasks */
  double reduceProgres;/**< the progress in reduce tasks */
};

/**\class JnomicsThriftException
*\brief Exception container for Jnomics Manager tasks
*/
struct JnomicsThriftException{
  string msg; /**< message from exception */
};

/**\class JnomicsCompute 
*\brief Service for Compute operations
* These are operations involving launching jobs on the cluster  
*/
struct JnomicsCompute{
        
  /**brief Align reads with Bowtie
   *\param inPath Input path in hdfs for reads to align (".pe,.se")
   *\param organism Reference genome to align to see listGenomes to obtain a list
   *\param outPath Output path in hdfs to write output from operation
   *\param opts Bowtie options
   *\param auth Authentication object containing user credentials
   *\return JnomicsThriftJobID job id for the running job
   *\exception JnomicsException containing error information for task
   */
  JnomicsThriftJobID alignBowtie ( string inPath, string organism,  string outPath,  string opts,  Authentication auth) throws JnomicsThriftException ;
 

  /**\brief Align reads with BWA
   *\param inPath Input path in hdfs for reads to align (".pe,.se")
   *\param organism Reference genome to align to see listGenomes to obtain a list
   *\param outPath Output path in hdfs to write output from operation
   *\param alignOpts alignment opts
   *\param sampeOpts opts for sampe
   *\param auth Authentication object containing user credentials
   *\return JnomicsThriftJobID job id for the running job
   *\exception JnomicsException containing error information for task
   */
  JnomicsThriftJobID alignBWA (string inPath, string organism, string outPath, string alignOpts, string sampeOpts, Authentication auth) throws JnomicsThriftException;
    
  /**\brief Call SNPs with Samtools
   * \param inPath Input path in hdfs for alignments (outpath of bwa/bowtie)
   * \param organism Reference genome to call SNPs against
   * \param outPath Output path in hdfs to write output from operation
   * \param auth Authentication object containing user credentials
   * \return JnomicsThriftJobID job id for the running job
   * \exception JnomicsException containing error information for task
   */
  JnomicsThriftJobID snpSamtools (string inPath, string organism, string outPath, Authentication auth) throws JnomicsThriftException;

  /**\brief Get the job status of Job
   * \param jobID ID of job
   * \param auth Authentication container
   * \exception JnomicsException containing error information for task
   */
  JnomicsThriftJobStatus getJobStatus(JnomicsThriftJobID jobID, Authentication auth) throws JnomicsThriftException;

  /**\brief Get a list of all of the jobs run by current user
   *\memberof JnomicsCompute
   * \param auth Authentication container
   * \exception JnomicsException containing error information for task
   */
  list<JnomicsThriftJobStatus> getAllJobs(Authentication auth) throws  JnomicsThriftException ;
        

  /**\brief merge vcf files 
   * \param inDir Input directory on hdfs with output from snp pipeline
   * \param inAlignments Input of alignments, output of bowtie/bwa
   * \param outVCF output file to store merged vcf entries
   * \param auth Authentication container
   * \exception JnomicsException containing error information for task
   */
  bool mergeVCF(string inDir, string inAlignments, string outVCF, Authentication auth) throws  JnomicsThriftException;
       
};

/** \class JnomicsData
* \brief Data operations for the Manager
* These are operations concerning data managment.
* Creation and manipulation of files in the user's 
* workspace 
*/
struct JnomicsData{
  
  /**\brief Create a file in hdfs
   * \param path Input directory on hdfs 
   * \param auth Authentication container
   * \returns JnomicsThriftHandle handle to the open file
   * \exception JnomicsException containing error information for task
   */
  JnomicsThriftHandle create ( string path, Authentication auth) throws JnomicsThriftException;

  /**\brief Open file in hdfs
   *\param path Path to open in hdfs
   *\param auth Authentication value
   *\returns JnomicsThriftHandle a handle to the open file
   *\exception JnomicsThriftException containing error information for task
   */
  JnomicsThriftHandle open (string path, Authentication auth) throws JnomicsThriftException;
  
  /**\brief Write data to file handle
   *\param handle a handle to the open file in hdfs
   *\param data binary data to write to the file
   *\param auth Authentication container
   *\exception JnomicsThriftException containing error information for task
   */
  void write (JnomicsThriftHandle handle, binary data, Authentication auth) throws JnomicsThriftException;

  /**\brief Read data from file handle
   *\param handle a handle to the open file in hdfs
   *\param auth Authentication container
   *\exception JnomicsThriftException containing error information for task
   */
  binary read (JnomicsThriftHandle handle, Authentication auth) throws JnomicsThriftException;

  /**\brief Close a file handle
   *\param handle a handle to the open file in hdfs
   *\param auth Authentication container
   *\exception JnomicsThriftException containing error information for task
   */
  void close(JnomicsThriftHandle handle, Authentication auth) throws  JnomicsThriftException;

  /**\brief List the status for files in hdfs
   *\param path the path in hdfs to return information about
   *\param auth Authentication container
   *\return list<JnoimcsThriftFileStatus> list of status for each file/directory in the path
   *\exception JnomicsThriftException containing error information for task
   */
  list<JnomicsThriftFileStatus> listStatus(string path, Authentication auth) throws JnomicsThriftException;

  /**\brief Remove path on hdfs
   *\param path the path in hdfs
   *\param recursive Remove recursively?
   *\param auth Authentication container
   *\return success/failure
   *\exception JnomicsThriftException containing error information for task
   */
  bool remove( string path, bool recursive, Authentication auth) throws JnomicsThriftException;
  
  /**\brief Make a directory in hdfs
   *\param path the path in hdfs
   *\param auth Authentication container
   *\return success/failure
   *\exception JnomicsThriftException containing error information for task
   */
  bool mkdir( string path, Authentication auth) throws JnomicsThriftException;
    
  /**\brief Move file/directory in hdfs
   *\param path the path in hdfs
   *\param dest destination path
   *\param auth Authentication container
   *\return success/failure
   *\exception JnomicsThriftException containing error information for task
   */
  bool mv(string path, string dest, Authentication auth) throws JnomicsThriftException;

  /**\brief List genomes that are indexed
   *\param auth Authentication container
   *\return list of genomes
   *\exception JnomicsThriftException containing error information for task
   */
  list<string> listGenomes(Authentication auth) throws JnomicsThriftException;
};

