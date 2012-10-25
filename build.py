#!/usr/bin/env python

import sys
import os


THIS_SCRIPT = os.path.dirname(os.path.realpath( __file__ ))

PATHS = {
    'SRC': "src/main/java",
    'OUT': "target",
    'LIB': "lib",
    'CLASSPATH': '',
    'OUT_JAR': 'jnomics-manager-0.3.jar'
}

def compile_jnomics():
    print "Compiling Jnomics"
    os.system("cd ./jnomics; ./build.py jar")
    os.system("cp jnomics/jnomics*.jar %s" % PATHS['LIB'])
    print "Done compiling Jnomics"
    print ""


for p in PATHS.iterkeys():
    PATHS[p] = os.path.join(THIS_SCRIPT, PATHS[p])

def getLibraries():
    return filter(lambda x: x.endswith(".jar"), os.listdir(PATHS['LIB']))

def compile(recur = 0):

    LIBRARIES = getLibraries()

    if(len(filter(lambda x: x.startswith("jnomics"),LIBRARIES)) < 1):
        if( recur > 0 ):
            sys.exit("Problem getting jnomics jar")
        compile_jnomics()
        return compile(1)

    CLASSPATH = ":".join(map(lambda x: os.path.join(PATHS['LIB'], x), LIBRARIES))

    print CLASSPATH
    
    SRC_FILE_TREE = []

    def vis(files,dirname,names):
        java_files = filter(lambda x: os.path.isfile(os.path.join(dirname,x)) and x.endswith(".java"), names)
        files += map(lambda x: os.path.join(dirname,x),java_files)
        return names

    os.path.walk(PATHS['SRC'], vis,SRC_FILE_TREE)

    SRC_FILES = " ".join( SRC_FILE_TREE ) 

    if not os.path.exists(PATHS['OUT']):
        os.mkdir(PATHS['OUT'])

    cmd = "javac -d %s -cp %s %s" % (PATHS['OUT'], CLASSPATH, SRC_FILES)
    print "Compiling %d files into %s" % (len(SRC_FILE_TREE), PATHS['OUT'])
    if(os.system(cmd) != 0):
        sys.exit(1)


def jar():
    if not os.path.exists(PATHS['OUT']):
        compile()
        
    out_jar = PATHS['OUT_JAR']
    if os.path.exists(out_jar):
        print "Found old jar: %s ... removing" % out_jar
        os.remove(out_jar)

    out_dir = PATHS['OUT']
        
    cmd = "jar -cf %s -C %s ."  % (out_jar,out_dir)

    #make fat jar
    LIBRARIES = map(lambda x: os.path.join(PATHS['LIB'], x), getLibraries())
    os.system('rm -rf %s' % os.path.join(PATHS['OUT'],"META-INF"))
    for lib in LIBRARIES:
            os.system("unzip -qo %s -d %s" % (lib,PATHS['OUT']))
            os.system('rm -rf %s' % os.path.join(PATHS['OUT'],"META-INF"))
    
    print "Building Jar %s " % out_jar
    os.system(cmd)


def clean():
    p = PATHS['OUT']
    print "Removing: %s " % p
    os.system('rm -rf %s' % p )

    print "Removing %s " % PATHS['OUT_JAR']
    os.system('rm %s '  % PATHS['OUT_JAR'] )

    print "Removing lib/jnomics.jar"
    os.system('rm lib/jnomics*.jar')

    print "Cleaning Jnomics"
    os.system('cd ./jnomics; ./build.py clean')


TASKS = { 'compile' : compile,
          'clean' : clean,
          'help' : help,
          'jar' : jar
          }



def help():
    print "Available Tasks:"
    for i in TASKS.iterkeys():
        print i
    
if __name__ == "__main__":

    if not len(sys.argv) > 1:
        help();sys.exit()

    task = sys.argv[1]

    if not TASKS.has_key(task):
        print "UNKNOWN TASK: %s" % task
        print help()
        sys.exit()
    else:
        TASKS[task]()
    


