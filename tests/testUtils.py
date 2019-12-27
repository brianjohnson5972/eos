from datetime import datetime
from datetime import timedelta
import re
import errno
import subprocess
import time
import os
import platform
from collections import deque
from collections import namedtuple
import inspect
import json
import shlex
import socket
from sys import stdout
from sys import exit
import traceback

###########################################################################################

def addEnum(enumClassType, type):
    setattr(enumClassType, type, enumClassType(type))

def unhandledEnumType(type):
    raise RuntimeError("No case defined for type=%s" % (type.type))

class EnumType:

    def __init__(self, type):
        self.type=type

    def __str__(self):
        return self.type


class ReturnType(EnumType):
    pass

addEnum(ReturnType, "raw")
addEnum(ReturnType, "json")

###########################################################################################

class BlockLogAction(EnumType):
    pass

addEnum(BlockLogAction, "make_index")
addEnum(BlockLogAction, "trim")
addEnum(BlockLogAction, "smoke_test")
addEnum(BlockLogAction, "return_blocks")

###########################################################################################
class Utils:
    Debug=False
    FNull = open(os.devnull, 'w')

    EosClientPath="programs/cleos/cleos"
    MiscEosClientArgs="--no-auto-keosd"

    EosWalletName="keosd"
    EosWalletPath="programs/keosd/"+ EosWalletName

    EosServerName="nodeos"
    EosServerPath="programs/nodeos/"+ EosServerName

    EosLauncherPath="programs/eosio-launcher/eosio-launcher"
    MongoPath="mongo"
    ShuttingDown=False
    CheckOutputDeque=deque(maxlen=10)

    EosBlockLogPath="programs/eosio-blocklog/eosio-blocklog"

    FileDivider="================================================================="
    DataDir="var/lib/"
    ConfigDir="etc/eosio/"

    @staticmethod
    def Print(*args, **kwargs):
        stackDepth=len(inspect.stack())-2
        s=' '*stackDepth
        stdout.write(s)
        print(*args, **kwargs)

    SyncStrategy=namedtuple("ChainSyncStrategy", "name id arg")

    SyncNoneTag="none"
    SyncReplayTag="replay"
    SyncResyncTag="resync"
    SyncHardReplayTag="hardReplay"

    SigKillTag="kill"
    SigTermTag="term"

    systemWaitTimeout=90
    irreversibleTimeout=60

    @staticmethod
    def setIrreversibleTimeout(timeout):
        Utils.irreversibleTimeout=timeout

    @staticmethod
    def setSystemWaitTimeout(timeout):
        Utils.systemWaitTimeout=timeout

    @staticmethod
    def getDateString(dt):
        return "%d_%02d_%02d_%02d_%02d_%02d" % (
            dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)

    @staticmethod
    def nodeExtensionToName(ext):
        r"""Convert node extension (bios, 0, 1, etc) to node name. """
        prefix="node_"
        if ext == "bios":
            return prefix + ext

        return "node_%02d" % (ext)

    @staticmethod
    def getNodeDataDir(ext, relativeDir=None, trailingSlash=False):
        path=os.path.join(Utils.DataDir, Utils.nodeExtensionToName(ext))
        if relativeDir is not None:
           path=os.path.join(path, relativeDir)
        if trailingSlash:
           path=os.path.join(path, "")
        return path

    @staticmethod
    def getNodeConfigDir(ext, relativeDir=None, trailingSlash=False):
        path=os.path.join(Utils.ConfigDir, Utils.nodeExtensionToName(ext))
        if relativeDir is not None:
           path=os.path.join(path, relativeDir)
        if trailingSlash:
           path=os.path.join(path, "")
        return path

    @staticmethod
    def getChainStrategies():
        chainSyncStrategies={}

        chainSyncStrategy=Utils.SyncStrategy(Utils.SyncNoneTag, 0, "")
        chainSyncStrategies[chainSyncStrategy.name]=chainSyncStrategy

        chainSyncStrategy=Utils.SyncStrategy(Utils.SyncReplayTag, 1, "--replay-blockchain")
        chainSyncStrategies[chainSyncStrategy.name]=chainSyncStrategy

        chainSyncStrategy=Utils.SyncStrategy(Utils.SyncResyncTag, 2, "--delete-all-blocks")
        chainSyncStrategies[chainSyncStrategy.name]=chainSyncStrategy

        chainSyncStrategy=Utils.SyncStrategy(Utils.SyncHardReplayTag, 3, "--hard-replay-blockchain")
        chainSyncStrategies[chainSyncStrategy.name]=chainSyncStrategy

        return chainSyncStrategies

    @staticmethod
    def checkOutput(cmd, ignoreError=False):
        if (isinstance(cmd, list)):
            popen=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            popen=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (output,error)=popen.communicate()
        Utils.CheckOutputDeque.append((output,error,cmd))
        if popen.returncode != 0 and not ignoreError:
            raise subprocess.CalledProcessError(returncode=popen.returncode, cmd=cmd, output=error)
        return output.decode("utf-8")

    @staticmethod
    def errorExit(msg="", raw=False, errorCode=1):
        if Utils.ShuttingDown:
            Utils.Print("ERROR:" if not raw else "", " errorExit called during shutdown, ignoring.  msg=", msg)
            return
        Utils.Print("ERROR:" if not raw else "", msg)
        traceback.print_stack(limit=-1)
        exit(errorCode)

    @staticmethod
    def cmdError(name, cmdCode=0):
        msg="FAILURE - %s%s" % (name, ("" if cmdCode == 0 else (" returned error code %d" % cmdCode)))
        Utils.Print(msg)

    @staticmethod
    def waitForObj(lam, timeout=None, sleepTime=3, reporter=None):
        if timeout is None:
            timeout=60

        endTime=time.time()+timeout
        needsNewLine=False
        try:
            while endTime > time.time():
                ret=lam()
                if ret is not None:
                    return ret
                if Utils.Debug:
                    Utils.Print("cmd: sleep %d seconds, remaining time: %d seconds" %
                                (sleepTime, endTime - time.time()))
                else:
                    stdout.write('.')
                    stdout.flush()
                    needsNewLine=True
                if reporter is not None:
                    reporter()
                time.sleep(sleepTime)
        finally:
            if needsNewLine:
                Utils.Print()

        return None

    @staticmethod
    def waitForBool(lam, timeout=None, sleepTime=3, reporter=None):
        myLam = lambda: True if lam() else None
        ret=Utils.waitForObj(myLam, timeout, sleepTime, reporter=reporter)
        return False if ret is None else ret

    @staticmethod
    def waitForBoolWithArg(lam, arg, timeout=None, sleepTime=3, reporter=None):
        myLam = lambda: True if lam(arg, timeout) else None
        ret=Utils.waitForObj(myLam, timeout, sleepTime, reporter=reporter)
        return False if ret is None else ret

    @staticmethod
    def filterJsonObjectOrArray(data):
        firstObjIdx=data.find('{')
        lastObjIdx=data.rfind('}')
        firstArrayIdx=data.find('[')
        lastArrayIdx=data.rfind(']')
        if firstArrayIdx==-1 or lastArrayIdx==-1:
            retStr=data[firstObjIdx:lastObjIdx+1]
        elif firstObjIdx==-1 or lastObjIdx==-1:
            retStr=data[firstArrayIdx:lastArrayIdx+1]
        elif lastArrayIdx < lastObjIdx:
            retStr=data[firstObjIdx:lastObjIdx+1]
        else:
            retStr=data[firstArrayIdx:lastArrayIdx+1]
        return retStr

    @staticmethod
    def runCmdArrReturnJson(cmdArr, trace=False, silentErrors=True):
        retStr=Utils.checkOutput(cmdArr)
        jStr=Utils.filterJsonObjectOrArray(retStr)
        if trace: Utils.Print ("RAW > %s" % (retStr))
        if trace: Utils.Print ("JSON> %s" % (jStr))
        if not jStr:
            msg="Received empty JSON response"
            if not silentErrors:
                Utils.Print ("ERROR: "+ msg)
                Utils.Print ("RAW > %s" % retStr)
            raise TypeError(msg)

        try:
            jsonData=json.loads(jStr)
            return jsonData
        except json.decoder.JSONDecodeError as ex:
            Utils.Print (ex)
            Utils.Print ("RAW > %s" % retStr)
            Utils.Print ("JSON> %s" % jStr)
            raise

    @staticmethod
    def runCmdReturnStr(cmd, trace=False):
        cmdArr=shlex.split(cmd)
        return Utils.runCmdArrReturnStr(cmdArr)


    @staticmethod
    def runCmdArrReturnStr(cmdArr, trace=False):
        retStr=Utils.checkOutput(cmdArr)
        if trace: Utils.Print ("RAW > %s" % (retStr))
        return retStr

    @staticmethod
    def runCmdReturnJson(cmd, trace=False, silentErrors=False):
        cmdArr=shlex.split(cmd)
        return Utils.runCmdArrReturnJson(cmdArr, trace=trace, silentErrors=silentErrors)

    @staticmethod
    def arePortsAvailable(ports):
        """Check if specified port (as int) or ports (as set) is/are available for listening on."""
        assert(ports)
        if isinstance(ports, int):
            ports={ports}
        assert(isinstance(ports, set))

        for port in ports:
            if Utils.Debug: Utils.Print("Checking if port %d is available." % (port))
            assert(isinstance(port, int))
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                s.bind(("127.0.0.1", port))
            except socket.error as e:
                if e.errno == errno.EADDRINUSE:
                    Utils.Print("ERROR: Port %d is already in use" % (port))
                else:
                    # something else raised the socket.error exception
                    Utils.Print("ERROR: Unknown exception while trying to listen on port %d" % (port))
                    Utils.Print(e)
                return False
            finally:
                s.close()

        return True

    @staticmethod
    def pgrepCmd(serverName):
        # pylint: disable=deprecated-method
        # pgrep differs on different platform (amazonlinux1 and 2 for example). We need to check if pgrep -h has -a available and add that if so:
        try:
            pgrepHelp = re.search('-a', subprocess.Popen("pgrep --help 2>/dev/null", shell=True, stdout=subprocess.PIPE).stdout.read().decode('utf-8'))
            pgrepHelp.group(0) # group() errors if -a is not found, so we don't need to do anything else special here.
            pgrepOpts="-a"
        except AttributeError as error:
            # If no -a, AttributeError: 'NoneType' object has no attribute 'group'
            pgrepOpts="-fl"

        return "pgrep %s %s" % (pgrepOpts, serverName)

    @staticmethod
    def getBlockLog(blockLogLocation, blockLogAction=BlockLogAction.return_blocks, outputFile=None, first=None, last=None, throwException=False, silentErrors=False, exitOnError=False):
        assert(isinstance(blockLogLocation, str))
        outputFileStr=" --output-file %s " % (outputFile) if outputFile is not None else ""
        firstStr=" --first %s " % (first) if first is not None else ""
        lastStr=" --last %s " % (last) if last is not None else ""

        blockLogActionStr=None
        returnType=ReturnType.raw
        if blockLogAction==BlockLogAction.return_blocks:
            blockLogActionStr=""
            returnType=ReturnType.json
        elif blockLogAction==BlockLogAction.make_index:
            blockLogActionStr=" --make-index "
        elif blockLogAction==BlockLogAction.trim:
            blockLogActionStr=" --trim "
        elif blockLogAction==BlockLogAction.smoke_test:
            blockLogActionStr=" --smoke-test "
        else:
            unhandledEnumType(blockLogAction)

        cmd="%s --blocks-dir %s --as-json-array %s%s%s%s" % (Utils.EosBlockLogPath, blockLogLocation, outputFileStr, firstStr, lastStr, blockLogActionStr)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        rtn=None
        try:
            if returnType==ReturnType.json:
                rtn=Utils.runCmdReturnJson(cmd, silentErrors=silentErrors)
            else:
                rtn=Utils.runCmdReturnStr(cmd)
        except subprocess.CalledProcessError as ex:
            if throwException:
                raise
            if not silentErrors:
                msg=ex.output.decode("utf-8")
                errorMsg="Exception during \"%s\". %s" % (cmd, msg)
                if exitOnError:
                    Utils.cmdError(errorMsg)
                    Utils.errorExit(errorMsg)
                else:
                    Utils.Print("ERROR: %s" % (errorMsg))
            return None

        if exitOnError and rtn is None:
            Utils.cmdError("could not \"%s\"" % (cmd))
            Utils.errorExit("Failed to \"%s\"" % (cmd))

        return rtn

    @staticmethod
    def compare(obj1,obj2,context):
        type1=type(obj1)
        type2=type(obj2)
        if type1!=type2:
            return "obj1(%s) and obj2(%s) are different types, so cannot be compared, context=%s" % (type1,type2,context)

        if obj1 is None and obj2 is None:
            return None

        typeName=type1.__name__
        if type1 == str or type1 == int or type1 == float or type1 == bool:
            if obj1!=obj2:
                return "obj1=%s and obj2=%s are different (type=%s), context=%s" % (obj1,obj2,typeName,context)
            return None

        if type1 == list:
            len1=len(obj1)
            len2=len(obj2)
            diffSizes=False
            minLen=len1
            if len1!=len2:
                diffSizes=True
                minLen=min([len1,len2])

            for i in range(minLen):
                nextContext=context + "[%d]" % (i)
                ret=Utils.compare(obj1[i],obj2[i], nextContext)
                if ret is not None:
                    return ret

            if diffSizes:
                return "left and right side %s comparison have different sizes %d != %d, context=%s" % (typeName,len1,len2,context)
            return None

        if type1 == dict:
            keys1=sorted(obj1.keys())
            keys2=sorted(obj2.keys())
            len1=len(keys1)
            len2=len(keys2)
            diffSizes=False
            minLen=len1
            if len1!=len2:
                diffSizes=True
                minLen=min([len1,len2])

            for i in range(minLen):
                key=keys1[i]
                nextContext=context + "[\"%s\"]" % (key)
                if key not in obj2:
                    return "right side does not contain key=%s (has %s) that left side does, context=%s" % (key,keys2,context)
                ret=Utils.compare(obj1[key],obj2[key], nextContext)
                if ret is not None:
                    return ret

            if diffSizes:
                return "left and right side %s comparison have different number of keys %d != %d, context=%s" % (typeName,len1,len2,context)

            return None

        return "comparison of %s type is not supported, context=%s" % (typeName,context)

###########################################################################################
class Account(object):
    # pylint: disable=too-few-public-methods

    def __init__(self, name):
        self.name=name

        self.ownerPrivateKey=None
        self.ownerPublicKey=None
        self.activePrivateKey=None
        self.activePublicKey=None


    def __str__(self):
        return "Name: %s" % (self.name)


###########################################################################################

class BlockData(object):
    # pylint: disable=too-few-public-methods

    TimeFmt = '%Y-%m-%dT%H:%M:%S.%f'
    SlotTime = 500
    SlotTimeSec = SlotTime / 1000
    SlotTimeLeeWaySec = SlotTimeSec / 5

    def __init__(self, num, id, producer, time, received, lib, trxs, rcvOrProduced, previous):
        self.num = num
        self.id = id
        self.producer = producer
        self.time = time
        self.received = received
        self.delay = received - time
        self.lib = lib
        self.trxs = trxs
        self.rcvOrProduced = rcvOrProduced
        self.previous = previous

        # time between the previous block being received last receive and this being received (will be set to delay for first block)
        # time added or removed to the cummulative delay of the previous block
        if previous is not None:
            self.timePassing = self.time - previous.time
            self.delayBetweenReceived = self.received - previous.received - self.timePassing
            self.cummulativeDelayChange = self.delayBetweenReceived - previous.delayBetweenReceived
            self.producerChanged = self.producer != previous.producer
            self.libChanged = self.lib - previous.lib
            self.producerSequence = 1 if self.producerChanged else previous.producerSequence + 1
        else:
            self.timePassing = timedelta(milliseconds = BlockData.SlotTime)
            self.delayBetweenReceived = self.delay
            self.cummulativeDelayChange = timedelta(milliseconds = 0)
            self.producerChanged = True
            self.libChanged = 0
            self.producerSequence = 1


    def __str__(self):
        libChanged = "(+%d)" % (self.libChanged) if self.libChanged else ""
        previousId = self.previous.id if self.previous is not None else "<first block in log>"
        return "num: %s, id: %s, producer: %s (%d), time: %s, rcvd: %s, delay: %s sec, lib: %s%s, trxs: %s, slot: %s, dbr: %s sec, delta: %s, previous id: %s, %s" % \
               (self.num, self.id, self.producer, self.producerSequence, self.time.strftime(BlockData.TimeFmt), self.received.strftime(BlockData.TimeFmt),
                self.delay.total_seconds(), self.lib, libChanged, self.trxs, BlockData.numSlots(self.timePassing.total_seconds()), self.delayBetweenReceived.total_seconds(),
                self.cummulativeDelayChange.total_seconds(), previousId, self.rcvOrProduced)

    @staticmethod
    def numSlots(timePassed):
        return int((timePassed + BlockData.SlotTimeLeeWaySec) / BlockData.SlotTimeSec)

###########################################################################################

class NodeosLogBlockReader:

    __lineStatusInterval = 5000
    __blockStatusInterval = 500

    def __init__(self, filename, forkAnalysisFilename=None, lineStatusInterval=__lineStatusInterval, blockStatusInterval=__blockStatusInterval):
        assert os.path.exists(filename), Utils.Print("ERROR: file %s doesn't exist" % (filename))
        Utils.Print("Opening: %s" % (filename))
        self.filename = filename
        self.file = open(filename)
        self.forkAnalysisFile = open(forkAnalysisFilename, "w") if forkAnalysisFilename is not None else None
        self.lineNum = 0
        self.lib = None
        self.blocks = []
        self.blockRegex = re.compile(r' (\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d\d\d) .* (Received|Produced) block (\w+)\.\.\. #(\d+) @ (\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d\d\d) signed by ([1-5\.a-z]+) \[trxs: (\d+), lib: (\d+),')
        self.lineStatusInterval = lineStatusInterval
        self.blockStatusInterval = blockStatusInterval

    def close(self):
        self.file.close()
        if self.forkAnalysisFile is not None:
            self.forkAnalysisFile.close()

    def next(self):
        match = None
        while match is None:
            line = self.file.readline()
            if len(line) == 0:
                if Utils.Debug: Utils.Print("Finished file: %s, read %d lines" % (self.filename, self.lineNum))
                return None
            self.lineNum += 1
            if Utils.Debug:
                if self.lineNum % self.lineStatusInterval == 0:
                    head = self.blocks[-1] if len(self.blocks) > 0 else None
                    blockNumDesc = ", at block num: %d (%s), lib: %s" % (head.num, head.id, head.lib) if head is not None else ""
                    Utils.Print("Read %d lines%s" % (self.lineNum, blockNumDesc))
            match = self.blockRegex.search(line)

        rcvTimeStr = match.group(1)
        rcvOrProduced = match.group(2)
        num = int(match.group(4))
        id = match.group(3)
        prodTimeStr = match.group(5)
        producer = match.group(6)
        trxs = int(match.group(7))
        lib = int(match.group(8))
        received = datetime.strptime(rcvTimeStr, BlockData.TimeFmt)
        time = datetime.strptime(prodTimeStr, BlockData.TimeFmt)

        # Utils.Print("\n\n")
        blocksLen = len(self.blocks)
        assert blocksLen == 0 or num <= self.blocks[-1].num + 1,\
            Utils.Print("ERROR: block number jumped from %d to %d. From line(%d): %s" % (self.blocks[-1].num, num, self.lineNum, line))
        toRemove = 0
        if self.lib is not None:
            toRemove = lib - self.lib
        elif blocksLen > 0 and lib > self.blocks[0].num:
            toRemove = lib - self.blocks[0].num
        block0 = str(self.blocks[0]) if blocksLen > 0 else "<no block 0>"
        blockLast = str(self.blocks[-1]) if blocksLen > 1 else "<no block -1>"
        # Utils.Print("This block ****")
        # Utils.Print("  num: %d, id: %s, lib: %d, producer: %s" % (num, id, lib, producer))
        assert self.lib is None or num >= self.lib, Utils.Print("ERROR: block num: %d is behind the lib: %d" % (num, lib))
        # Utils.Print("ToRemove ****")
        # Utils.Print("blockLen: %s, toRemove: %s, blocks[0]: %s, block[-1]: %s" % (blocksLen, toRemove, block0, blockLast))
        assert toRemove >= 0, Utils.Print("ERROR: Previous block had lib: %d, but new block has lib: %d.  It is not valid to have lib reduced ever. From line(%d): %s" % (self.lib, lib, self.lineNum, line))
        if toRemove > 0:
            assert blocksLen >= toRemove + 1, Utils.Print("ERROR: Previous block had lib: %d, and new block has lib: %d, but there have only been %d blocks received since the previous lib. From line(%d): %s" % (self.lib, lib, blocksLen, self.lineNum, line))
            assert self.blocks[toRemove].num == lib, Utils.Print("ERROR: Previous block had lib: %d, and new block has lib: %d, but received blocks are not consistent with that. %d blocks after last lib has block num: %d. From line(%d): %s" % (self.lib, lib, toRemove, self.blocks[toRemove].num, self.lineNum, line))
            if Utils.Debug: Utils.Print("Moving lib from %s to %s" % (self.lib, lib))
            # if lib + 1 == num:
            #     # handle corner case for single producer networks
            #     self.blocks = self.blocks[toRemove - 1:toRemove - 1]
            # else:d
            self.blocks = self.blocks[toRemove:]

        blocksLen = len(self.blocks)
        class ForkData:
            def __init__(self):
                self.accummulatedSlots = 0
                self.expectedSlots = 0
                self.trxs = 0
                self.time = None

            def add(self, block):
                slots = BlockData.numSlots(block.timePassing.total_seconds())
                self.accummulatedSlots += slots
                self.expectedSlots += 1
                self.trxs += block.trxs
                if self.time is None:
                    self.time = block.time

            def missedSlots(self):
                return self.accummulatedSlots - self.expectedSlots

        block0 = str(self.blocks[0]) if blocksLen > 0 else "<no block 0>"
        blockLast = str(self.blocks[-1]) if blocksLen > 1 else "<no block -1>"
        # Utils.Print("Possible Rollback ****")
        # Utils.Print("blockLen: %s, blocks[0]: %s, block[-1]: %s" % (blocksLen, block0, blockLast))

        printHeadForkAnalysis = None
        if blocksLen > 1:
            prevHead = self.blocks[-1].num

            blockNumChange = num - prevHead
            assert blockNumChange <= 1, Utils.Print("ERROR: Current block number indicates: %d, but the previous head was: %d, block numbers cannot skip. From line(%d): %s" %
                                                    (num, prevHead, self.lineNum, line))

            remainingBlocks = blocksLen + blockNumChange
            # Utils.Print("num: %d, prevHead: %d, blocksLen: %d, blockNumChange: %d, remainingBlocks: %d" % (num, prevHead, blocksLen, blockNumChange, remainingBlocks))
            assert self.lib is None or remainingBlocks > 0,\
                   Utils.Print("ERROR: Current block number indicates %d is the previous block, previous head is block num %d, blocks length is %d. From line(%d): %s" %
                               (num - 1, prevHead, blocksLen, self.lineNum, line))

            if blockNumChange < 1:
                # remove 1 or more of trailing blocks so that
                reverseIndex = blockNumChange - 1
                if self.lib is None and reverseIndex * -1 >= blocksLen:
                    if Utils.Debug: Utils.Print("Rolling head back from %d and dropping all previous blocks" %
                                                (prevHead))
                    if self.forkAnalysisFile:
                        self.forkAnalysisFile.write("%s\nOld Fork: (dropping all %d ** not at lib yet)\n" % (Utils.FileDivider, blocksLen))
                        for block in self.blocks:
                            self.forkAnalysisFile.write("   %s\n" % (str(block)))
                        self.forkAnalysisFile.write("New Fork:\n")
                        printHeadForkAnalysis = ForkData()
                    self.blocks = []
                else:
                    assert reverseIndex * -1 < blocksLen, \
                           Utils.Print("ERROR: Current block number indicates: %d is the previous block, which is %d behind our current head and we don't have that many blocks. From line(%d): %s" %
                                       (num - 1, reverseIndex * -1, self.lineNum, line))
                    if Utils.Debug: Utils.Print("Rolling head back %d blocks from %d to %d (id: %s) and then adding this block on" %
                                                (reverseIndex * -1, prevHead, self.blocks[reverseIndex - 1].num, self.blocks[reverseIndex - 1].id))
                    if self.forkAnalysisFile:
                        self.forkAnalysisFile.write("%s\nOld Fork (dropping %d):\n" % (Utils.FileDivider, reverseIndex * -1))
                        printHeadForkAnalysis = ForkData()
                        for block in self.blocks[reverseIndex:]:
                            self.forkAnalysisFile.write("   %s\n" % (str(block)))
                            printHeadForkAnalysis.add(block)
                        self.forkAnalysisFile.write("New Fork:\n")
                    self.blocks = self.blocks[:reverseIndex]

        if Utils.Debug:
            if num % self.blockStatusInterval == 0:
                Utils.Print("At block num: %d" % (num))

        previous = self.blocks[-1] if len(self.blocks) > 0 else None
        # Utils.Print("blockLen: %s, previous: %s" % (len(self.blocks), str(previous)))
        current = BlockData(num, id, producer, time, received, lib, trxs, rcvOrProduced, previous)
        self.blocks.append(current)
        # Utils.Print("Post-info ****")
        # Utils.Print("blocks[0].num: %s, lib: %s" % (self.blocks[0].num, lib))
        if self.blocks[0].num == lib:
            self.lib = lib
        else:
            assert self.lib is None,\
                Utils.Print("ERROR: Last stored block was tracking lib, but it is not anymore.  " +
                            "Previous lib identified as oldest in block list: %d.  " +
                            "Now oldest block num: %d. Newest block num: %d and lib: %d" %
                            (self.lib, self.blocks[0].num, current.num, current.lib))
            Utils.Print("NOT setting lib")
        if printHeadForkAnalysis:
            self.forkAnalysisFile.write("   %s\n" % (str(current)))
            if printHeadForkAnalysis.accummulatedSlots > 0.0:
                newForkSlots = BlockData.numSlots(current.timePassing.total_seconds())
                if current.time < printHeadForkAnalysis.time:
                    earlierLater = "earlier"
                    newForkMissingSlots = newForkSlots - 1
                else:
                    earlierLater = "later"
                    newForkMissingSlots = newForkSlots - printHeadForkAnalysis.accummulatedSlots - 1

                self.forkAnalysisFile.write("\nOld Fork skipped %d slots and contained %d total transactions.\n" % (printHeadForkAnalysis.missedSlots(), printHeadForkAnalysis.trxs))
                self.forkAnalysisFile.write("New Fork skipped %d slots and contained %d total transactions. It is %s than the old fork.\n" % (newForkMissingSlots, current.trxs, earlierLater))
                trxsDiff = current.trxs - printHeadForkAnalysis.trxs
                lossGain = "loss" if trxsDiff < 0 else "gain"
                trxsPercent = "%d%% %s" % (int((trxsDiff / printHeadForkAnalysis.trxs) * 100), lossGain) if printHeadForkAnalysis.trxs != 0 else ""
                self.forkAnalysisFile.write("%d transaction change. %s\n\n" % (trxsDiff, trxsPercent))

        return current