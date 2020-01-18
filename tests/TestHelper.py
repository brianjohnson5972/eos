from testUtils import Utils
from Cluster import Cluster
from WalletMgr import WalletMgr
from datetime import datetime
from datetime import timedelta
from testUtils import EnumType
from testUtils import addEnum
import platform
import os
import re
import threading
import time

import argparse

class AppArgs:
    def __init__(self):
        self.args=[]

    class AppArg:
        def __init__(self, flag, help, type=None, default=None, choices=None, action=None):
            self.flag=flag
            self.type=type
            self.help=help
            self.default=default
            self.choices=choices
            self.action=action

    def add(self, flag, type, help, default, choices=None):
        arg=self.AppArg(flag, help, type=type, default=default, choices=choices)
        self.args.append(arg)


    def add_bool(self, flag, help, action='store_true'):
        arg=self.AppArg(flag, help, action=action)
        self.args.append(arg)

class TimerProcess:
    class OverlapType(EnumType):
        pass

    # in reference to the timespan data the OverlapType is on
    addEnum(OverlapType, "begin")
    addEnum(OverlapType, "end")
    # timespan data is contained in other timespan
    addEnum(OverlapType, "contained")
    # timespan data encompasses other timespan
    addEnum(OverlapType, "encompass")
    # timespan data overlaps somewhere
    addEnum(OverlapType, "any")

    class MissedTimeDescriptor:
        def __init_(self, start, stop, overlapType):
            assert(isinstance(overlapType, OverlapType))
            self.start=start
            self.stop=stop
            self.overlapType=overlapType

        def check(self, start, stop):
            def asTime(time):
                if isintance(time, str):
                    time=datetime.strptime(time, Utils.TimeFmt)
                else:
                    assert isinstance(time, datetime)
                return time

            if self.overlapType == OverlapType.begin:
                return start <= test.start and test.start <= end
            elif self.overlapType == OverlapType.end:
                return start <= test.end and test.end <= end
            elif self.overlapType == OverlapType.contains:
                return start <= test.start and test.end <= end
            elif self.overlapType == OverlapType.encompass:
                return test.start <= start and end <= test.end
            elif self.overlapType == OverlapType.any:
                return (test.start <= start and start <= test.end) or \
                       (start <= test.start and test.start <= end) or \
                       (test.start <= end and end <= test.end) or \
                       (start <= test.end and test.end <= end)
            assert True, Utils.errorExit("No code to support overlapType: %s" % (self.overlapType))

    def __init__(self, outfilename, interval=0.1):
        self.interval=interval
        self.outfilename=outfilename
        self.outfile=open(outfilename,'w')
        self.procLock=threading.Lock()
        self.run=True
        self.exited=False
        self.analysis=None
        self.expectedWindow=timedelta(milliseconds=100)
        self.thread=threading.Thread(target=self.reportTime, args=())
        self.thread.start()

    def shutdown(self):
        with self.procLock:
            if Utils.Debug: Utils.Print("shutting down timer process")
            self.run=False
        self.thread.join()
        if Utils.Debug: Utils.Print("shutdown timer process")

    def __analyzeWindow(self, previous, current, report):
        diffTime=current - previous if previous else self.expectedWindow

        missedStr="missed"
        multiplier=2
        if diffTime < multiplier * self.expectedWindow:
            return (True, diffTime)

        if missedStr not in self.analysis:
            self.analysis[missedStr]=[]

        self.analysis[missedStr].append((previous,current))

        if report:
            percent=int((diffTime - self.expectedWindow) / self.expectedWindow * 100)
            Utils.Print("No timing info during %s window (from %s to %s, exceeded expected time by %d%%)" % (diffTime, previous, current, percent))

        return (False, diffTime)

    def analyze(self, missedTimeWindows=("",[]), report=False, failOnError=False):
        assert self.exited, Utils.Print("ERROR: Cannot call TimerProcess.analyze() before shutdown has been called")
        assert(isinstance(missedTimeWindows, tuple))
        assert(len(missedTimeWindows) == 2)
        assert(isinstance(missedTimeWindows[0], str))
        assert(isinstance(missedTimeWindows[1], list))

        # get rid of the tuple
        missedTimeWindowsDesc=missedTimeWindows[0]
        missedTimeWindows=missedTimeWindows[1]
        if Utils.Debug:
            report = True
        self.analysis={}
        previous=None
        with open(self.outfilename, 'r') as analyzeFile:
            timeReg=re.compile(r'\b(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d\d\d(:?\d\d\d)?)\b')
            maxDiff=timedelta(milliseconds=0)
            missedTimerWindowCount=0
            for line in analyzeFile.readlines():
                match=timeReg.search(line)
                if match:
                    lineTime=datetime.strptime(match.group(1), Utils.TimeFmt)
                    (status, diffTime) = self.__analyzeWindow(previous, lineTime, report)
                    if maxDiff < diffTime:
                        maxDiff=diffTime
                    if not status:
                        missedTimerWindowCount+=1
                    previous=lineTime

        if Utils.Debug: Utils.Print("Timing reporting indicates %d times the system was not getting clock time.  "
                                    "The largest delay in the timing tool reported was: %.2f seconds." %
                                    (missedTimerWindowCount, maxDiff.total_seconds()))

        if "missed" in self.analysis:
            missingTimes=self.analysis["missed"]
            indent="   "
            failed=False
            unexplainedMissing=[]
            explainedMissing=[]
            for missedTimeWindow in missedTimeWindows:
                found=False
                for missingTime in missingTimes:
                    if missedTimeWindow.end < missingTime[0]:
                        break
                    elif missedTimeWindow.check(missingTime[0], missingTime[1]):
                        found=True
                        explainedMissing.append((missedTimeWindow, missingTime))
                        break
                if not found:
                    unexplainedMissing.append(missedTimeWindow)
                    failed=True
            if len(explainedMissing) > 0:
                Utils.Print("Missing %s in the following time windows but are expected due to missing time reporting information:" % (missedTimeWindowsDesc))
                for missing in explainedMissing:
                    (missedTimeWindow, missingTime) = missing
                    Utils.Print("%s%s to %s explained by %s to %s time gap" % (indent, missedTimeWindow.start, missedTimeWindow.end, missingTime[0], missingTime[1]))
            if failed:
                Utils.Print("ERROR: Missing %s in the following time windows:" % (missedTimeWindowsDesc))
                for missedTimeWindow in unexplainedMissing:
                    Utils.Print("%s%s to %s" % (indent, missedTimeWindow.start, missedTimeWindow.end))

            if failed:
                Utils.errorExit("See above errors.")

            if failOnError or report:
                missed = "Missed the following time windows:\n"
                for miss in missingTimes:
                    missed += "%s%s to %s\n" % (indent, miss[0], miss[1])
                missed += "\n"
                if failOnError: Utils.errorExit(missed)
                elif report: Utils.Print(missed)

    def reportTime(self):
        try:
            Utils.Print("Starting reportTime")
            while True:
                with self.procLock:
                    if not self.run:
                        self.outfile.write(datetime.utcnow().strftime(Utils.TimeFmt) + ' exiting\n')
                        self.exited=True
                        return
                self.outfile.write(datetime.utcnow().strftime(Utils.TimeFmt) + '\n')
                time.sleep(0.1)
        finally:
            self.outfile.close()
            Utils.Print("Stopping reportTime")

# pylint: disable=too-many-instance-attributes
class TestHelper(object):
    LOCAL_HOST="localhost"
    DEFAULT_PORT=8888
    DEFAULT_WALLET_PORT=9899
    TIMER_PROC=None

    @staticmethod
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    def parse_args(includeArgs, applicationSpecificArgs=AppArgs()):
        """Accepts set of arguments, builds argument parser and returns parse_args() output."""
        assert(includeArgs)
        assert(isinstance(includeArgs, set))
        assert(isinstance(applicationSpecificArgs, AppArgs))

        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument('-?', action='help', default=argparse.SUPPRESS,
                                 help=argparse._('show this help message and exit'))

        if "-p" in includeArgs:
            parser.add_argument("-p", type=int, help="producing nodes count", default=1)
        if "-n" in includeArgs:
            parser.add_argument("-n", type=int, help="total nodes", default=0)
        if "-d" in includeArgs:
            parser.add_argument("-d", type=int, help="delay between nodes startup", default=1)
        if "--nodes-file" in includeArgs:
            parser.add_argument("--nodes-file", type=str, help="File containing nodes info in JSON format.")
        if "-s" in includeArgs:
            parser.add_argument("-s", type=str, help="topology", choices=["mesh"], default="mesh")
        if "-c" in includeArgs:
            parser.add_argument("-c", type=str, help="chain strategy",
                    choices=[Utils.SyncResyncTag, Utils.SyncReplayTag, Utils.SyncNoneTag, Utils.SyncHardReplayTag],
                    default=Utils.SyncResyncTag)
        if "--kill-sig" in includeArgs:
            parser.add_argument("--kill-sig", type=str, choices=[Utils.SigKillTag, Utils.SigTermTag], help="kill signal.",
                    default=Utils.SigKillTag)
        if "--kill-count" in includeArgs:
            parser.add_argument("--kill-count", type=int, help="nodeos instances to kill", default=-1)
        if "--terminate-at-block" in includeArgs:
            parser.add_argument("--terminate-at-block", type=int, help="block to terminate on when replaying", default=0)
        if "--seed" in includeArgs:
            parser.add_argument("--seed", type=int, help="random seed", default=1)

        if "--host" in includeArgs:
            parser.add_argument("-h", "--host", type=str, help="%s host name" % (Utils.EosServerName),
                                     default=TestHelper.LOCAL_HOST)
        if "--port" in includeArgs:
            parser.add_argument("--port", type=int, help="%s host port" % Utils.EosServerName,
                                     default=TestHelper.DEFAULT_PORT)
        if "--wallet-host" in includeArgs:
            parser.add_argument("--wallet-host", type=str, help="%s host" % Utils.EosWalletName,
                                     default=TestHelper.LOCAL_HOST)
        if "--wallet-port" in includeArgs:
            parser.add_argument("--wallet-port", type=int, help="%s port" % Utils.EosWalletName,
                                     default=TestHelper.DEFAULT_WALLET_PORT)
        if "--prod-count" in includeArgs:
            parser.add_argument("-c", "--prod-count", type=int, help="Per node producer count", default=1)
        if "--defproducera_prvt_key" in includeArgs:
            parser.add_argument("--defproducera_prvt_key", type=str, help="defproducera private key.")
        if "--defproducerb_prvt_key" in includeArgs:
            parser.add_argument("--defproducerb_prvt_key", type=str, help="defproducerb private key.")
        if "--mongodb" in includeArgs:
            parser.add_argument("--mongodb", help="Configure a MongoDb instance", action='store_true')
        if "--dump-error-details" in includeArgs:
            parser.add_argument("--dump-error-details",
                                     help="Upon error print etc/eosio/node_*/config.ini and var/lib/node_*/stderr.log to stdout",
                                     action='store_true')
        if "--dont-launch" in includeArgs:
            parser.add_argument("--dont-launch", help="Don't launch own node. Assume node is already running.",
                                     action='store_true')
        if "--keep-logs" in includeArgs:
            parser.add_argument("--keep-logs", help="Don't delete var/lib/node_* folders upon test completion",
                                     action='store_true')
        if "-v" in includeArgs:
            parser.add_argument("-v", help="verbose logging", action='store_true')
        if "--leave-running" in includeArgs:
            parser.add_argument("--leave-running", help="Leave cluster running after test finishes", action='store_true')
        if "--only-bios" in includeArgs:
            parser.add_argument("--only-bios", help="Limit testing to bios node.", action='store_true')
        if "--clean-run" in includeArgs:
            parser.add_argument("--clean-run", help="Kill all nodeos and kleos instances", action='store_true')
        if "--sanity-test" in includeArgs:
            parser.add_argument("--sanity-test", help="Validates nodeos and kleos are in path and can be started up.", action='store_true')
        if "--alternate-version-labels-file" in includeArgs:
            parser.add_argument("--alternate-version-labels-file", type=str, help="Provide a file to define the labels that can be used in the test and the path to the version installation associated with that.")
        if "--suppress-timer-process" in includeArgs:
            parser.add_argument("--suppress-timer-process", help="prevent the tests timer process from running during the test", action='store_true')

        for arg in applicationSpecificArgs.args:
            if arg.type is not None:
                parser.add_argument(arg.flag, type=arg.type, help=arg.help, choices=arg.choices, default=arg.default)
            else:
                parser.add_argument(arg.flag, help=arg.help, action=arg.action)

        args = parser.parse_args()
        if not hasattr(args, "suppress_timer_process") or not args.suppress_timer_process:
            outfilename=os.path.join(Utils.DataDir, "100millisecTimeFile.txt")
            TestHelper.TIMER_PROC=TimerProcess(outfilename)
        return args

    @staticmethod
    def printSystemInfo(prefix):
        """Print system information to stdout. Print prefix first."""
        if prefix:
            Utils.Print(str(prefix))
        clientVersion=Cluster.getClientVersion()
        Utils.Print("UTC time: %s" % str(datetime.utcnow()))
        Utils.Print("EOS Client version: %s" % (clientVersion))
        Utils.Print("Processor: %s" % (platform.processor()))
        Utils.Print("OS name: %s" % (platform.platform()))
    
    @staticmethod
    # pylint: disable=too-many-arguments
    def shutdown(cluster, walletMgr, testSuccessful=True, killEosInstances=True, killWallet=True, keepLogs=False, cleanRun=True, dumpErrorDetails=False, missedTimeWindows=("",[])):
        """Cluster and WalletMgr shutdown and cleanup."""
        assert(cluster)
        assert(isinstance(cluster, Cluster))
        if walletMgr:
            assert(isinstance(walletMgr, WalletMgr))
        assert(isinstance(testSuccessful, bool))
        assert(isinstance(killEosInstances, bool))
        assert(isinstance(killWallet, bool))
        assert(isinstance(cleanRun, bool))
        assert(isinstance(dumpErrorDetails, bool))

        Utils.ShuttingDown=True

        if testSuccessful:
            Utils.Print("Test succeeded.")
        else:
            Utils.Print("Test failed.")

        if TestHelper.TIMER_PROC:
            TestHelper.TIMER_PROC.shutdown()
            TestHelper.TIMER_PROC.analyze(missedTimeWindows, report=True)

        if not testSuccessful and dumpErrorDetails:
            cluster.reportStatus()
            Utils.Print(Utils.FileDivider)
            psOut=Cluster.pgrepEosServers(timeout=60)
            Utils.Print("pgrep output:\n%s" % (psOut))
            cluster.dumpErrorDetails()
            if walletMgr:
                walletMgr.dumpErrorDetails()
            cluster.printBlockLogIfNeeded()
            Utils.Print("== Errors see above ==")
            if len(Utils.CheckOutputDeque)>0:
                Utils.Print("== cout/cerr pairs from last %d calls to Utils. ==" % len(Utils.CheckOutputDeque))
                for out, err, cmd in reversed(Utils.CheckOutputDeque):
                    Utils.Print("cmd={%s}" % (" ".join(cmd)))
                    Utils.Print("cout={%s}" % (out))
                    Utils.Print("cerr={%s}\n" % (err))
                Utils.Print("== cmd/cout/cerr pairs done. ==")

        if killEosInstances:
            Utils.Print("Shut down the cluster.")
            cluster.killall(allInstances=cleanRun)
            if testSuccessful and not keepLogs:
                Utils.Print("Cleanup cluster data.")
                cluster.cleanup()

        if walletMgr and killWallet:
            Utils.Print("Shut down the wallet.")
            walletMgr.killall(allInstances=cleanRun)
            if testSuccessful and not keepLogs:
                Utils.Print("Cleanup wallet data.")
                walletMgr.cleanup()

