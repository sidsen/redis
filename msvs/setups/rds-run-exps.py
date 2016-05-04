import os
import sys
import getopt
import subprocess
import re

# Global constants
serverCmd = './redis-server.exe --maxheap 200mb --wm 1 --exp-trials 3 --exp-duration-us 2000000'
serverCmdFCRW = './redis-server-fcrw.exe --maxheap 200mb --wm 1 --exp-trials 3 --exp-duration-us 2000000'
serverCmdRWL = './redis-server-rwl.exe --maxheap 200mb --wm 1 --exp-trials 3 --exp-duration-us 2000000'
serverCmdSL = './redis-server-sl.exe --maxheap 200mb --wm 1 --exp-trials 3 --exp-duration-us 2000000'
clientCmd = './redis-benchmark.exe -t zadd -c 100 -P 100 -n 200000 -r 10000 -q'
clientCmd2 = './redis-benchmark.exe -t zmixed -c 250 -P 1 -n 3000 -r 10000 -f 50 -q'


def errorExit(error):
    print error + "\n"
    sys.exit(1)

def usage():
    usageStr = "Usage: \n\n"
    usageStr = usageStr + "python ./rds-run-exps.py\n"
    return usageStr

# Runs redis clients in a blocking manner
def runClient():
    cli = subprocess.call(clientCmd.split(), shell=False)
    # Sleep for a bit to let the redis db settle
    cli = subprocess.call('sleep 0.1'.split(), shell=False)
    cli = subprocess.call(clientCmd2.split(), shell=False)

# Runs redis server in a non-blocking way, returns a handle to the subprocess
def runServer(cmd, threads, mode):
    cmd = serverCmd + ' --threads ' + str(threads + 1) + ' ' + mode
    print "Running command: " + cmd
    return subprocess.Popen(cmd.split(),
                            shell=False,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

def updateStats(threads, results, output):
    nums = re.findall("\d+\.\d+ ops/sec", output)
    readRatios = re.findall("read ratio = \d+\.\d+", output)
    i = 0
    for ratio in readRatios:
        r = ratio.split()[-1]
        if r not in results:
            results[r] = [ [ threads, nums[i].split()[0] ] ]
        else:
            results[r].append( [ threads, nums[i].split()[0] ] )
        i += 1

def printStats(threadCnts, results):
    header = 'Threads\t'
    readRatios = sorted(results);
    # Build and print the table header
    for ratio in readRatios:
        header += ratio + '\t'
    print header
    i = 0
    for threads in threadCnts:
        line = str(threads) + '\t'
        for ratio in readRatios:
            assert(results[ratio][i][0] == int(threads))
            line += str(results[ratio][i][1]) + '\t'
        print line
        i += 1
    print '\n'
            
def main(argv):
    # Parse the command-line arguments.
    try:
        opts, args = getopt.getopt(argv, " ", [])
    except getopt.GetoptError:
        errorExit("Error parsing command line options\n" + usage())

    for opt, arg in opts:
        pass
        #if opt == "-a"
        #else:
        #    errorExit("Unexpected command line argument " + opt + "\n" + usage())

    threadCnts = [ 1, 6, 12, 18, 24, 30, 36, 42, 48 ]
    throughputP = re.compile('\"(.+?)\".*?\"et\":\"(.+?)\"')

    rdsResults = {}
    fcResults = {}
    fcrwResults = {}
    rwlResults = {}
    slResults = {}
    lockResults = {}
    for threads in threadCnts:
        rs = runServer(serverCmd, threads, '--repl')
        # This call blocks; when its done, we know the server is done too
        runClient();
        stdout, stderr = rs.communicate()
        updateStats(threads, rdsResults, stdout)

        rs = runServer(serverCmd, threads, '--fc')
        # This call blocks; when its done, we know the server is done too
        runClient();
        stdout, stderr = rs.communicate()
        updateStats(threads, fcResults, stdout)

        rs = runServer(serverCmdFCRW, threads, '--fc')
        # This call blocks; when its done, we know the server is done too
        runClient();
        stdout, stderr = rs.communicate()
        updateStats(threads, fcrwResults, stdout)

        rs = runServer(serverCmdRWL, threads, '--fc')
        # This call blocks; when its done, we know the server is done too
        runClient();
        stdout, stderr = rs.communicate()
        updateStats(threads, rwlResults, stdout)

        rs = runServer(serverCmdSL, threads, '--fc')
        # This call blocks; when its done, we know the server is done too
        runClient();
        stdout, stderr = rs.communicate()
        updateStats(threads, slResults, stdout)

        rs = runServer(threads, '')
        # This call blocks; when its done, we know the server is done too
        runClient();
        stdout, stderr = rs.communicate()
        updateStats(threads, lockResults, stdout)

    # Print results
    print "\n=============== RESULTS ===============\n\n"
    print "RDS results:\n"
    printStats(threadCnts, rdsResults)
    print "FC results:\n"
    printStats(threadCnts, fcResults)
    print "FCRW results:\n"
    printStats(threadCnts, fcrwResults)
    print "RWL results:\n"
    printStats(threadCnts, rwlResults)
    print "SL results:\n"
    printStats(threadCnts, slResults)
    print "Lock results:\n"
    printStats(threadCnts, lockResults)
    

if __name__ == "__main__":
    # Note this strips off the command name.
    main(sys.argv[1:])
