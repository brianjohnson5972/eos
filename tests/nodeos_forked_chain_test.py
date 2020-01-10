#!/usr/bin/env python3

from testUtils import Utils
from datetime import datetime
from datetime import timedelta
import time
from Cluster import Cluster
from WalletMgr import WalletMgr
from TestHelper import TestHelper

import json
import signal

###############################################################
# nodeos_forked_chain_test
# 
# This test sets up 2 producing nodes and one "bridge" node using test_control_api_plugin.
#   One producing node has 11 of the elected producers and the other has 10 of the elected producers.
#   All the producers are named in alphabetical order, so that the 11 producers, in the one production node, are
#       scheduled first, followed by the 10 producers in the other producer node. Each producing node is only connected
#       to the other producing node via the "bridge" node.
#   The bridge node has the test_control_api_plugin, which exposes a restful interface that the test script uses to kill
#       the "bridge" node at a specific producer in the production cycle. This is used to fork the producer network
#       precisely when the 11 producer node has finished producing and the other producing node is about to produce.
#   The fork in the producer network results in one fork of the block chain that advances with 10 producers with a LIB
#      that has advanced, since all of the previous blocks were confirmed and the producer that was scheduled for that
#      slot produced it, and one with 11 producers with a LIB that has not advanced.  This situation is validated by
#      the test script.
#   After both chains are allowed to produce, the "bridge" node is turned back on.
#   Time is allowed to progress so that the "bridge" node can catchup and both producer nodes to come to consensus
#   The block log is then checked for both producer nodes to verify that the 10 producer fork is selected and that
#       both nodes are in agreement on the block log.
#
###############################################################

Print=Utils.Print

from core_symbol import CORE_SYMBOL

def analyzeBPs(bps0, bps1, expectDivergence):
    start=0
    index=None
    length=len(bps0)
    firstDivergence=None
    errorInDivergence=False
    analysysPass=0
    bpsStr=None
    bpsStr0=None
    bpsStr1=None
    while start < length:
        analysysPass+=1
        bpsStr=None
        for i in range(start,length):
            bp0=bps0[i]
            bp1=bps1[i]
            if bpsStr is None:
                bpsStr=""
            else:
                bpsStr+=", "
            blockNum0=bp0["blockNum"]
            prod0=bp0["prod"]
            blockNum1=bp1["blockNum"]
            prod1=bp1["prod"]
            numDiff=True if blockNum0!=blockNum1 else False
            prodDiff=True if prod0!=prod1 else False
            if numDiff or prodDiff:
                index=i
                if firstDivergence is None:
                    firstDivergence=min(blockNum0, blockNum1)
                if not expectDivergence:
                    errorInDivergence=True
                break
            bpsStr+=str(blockNum0)+"->"+prod0

        if index is None:
            if expectDivergence:
                errorInDivergence=True
                break
            return None

        bpsStr0=None
        start=length
        for i in range(index,length):
            if bpsStr0 is None:
                bpsStr0=""
                bpsStr1=""
            else:
                bpsStr0+=", "
                bpsStr1+=", "
            bp0=bps0[i]
            bp1=bps1[i]
            blockNum0=bp0["blockNum"]
            prod0=bp0["prod"]
            blockNum1=bp1["blockNum"]
            prod1=bp1["prod"]
            numDiff="*" if blockNum0!=blockNum1 else ""
            prodDiff="*" if prod0!=prod1 else ""
            if not numDiff and not prodDiff:
                start=i
                index=None
                if expectDivergence:
                    errorInDivergence=True
                break
            bpsStr0+=str(blockNum0)+numDiff+"->"+prod0+prodDiff
            bpsStr1+=str(blockNum1)+numDiff+"->"+prod1+prodDiff
        if errorInDivergence:
            break

    if errorInDivergence:
        msg="Failed analyzing block producers - "
        if expectDivergence:
            msg+="nodes do not indicate different block producers for the same blocks, but they are expected to diverge at some point."
        else:
            msg+="did not expect nodes to indicate different block producers for the same blocks."
        msg+="\n  Matching Blocks= %s \n  Diverging branch node0= %s \n  Diverging branch node1= %s" % (bpsStr,bpsStr0,bpsStr1)
        Utils.errorExit(msg)

    return firstDivergence

def getMinHeadAndLib(prodNodes):
    info0=prodNodes[0].getInfo(exitOnError=True)
    info1=prodNodes[1].getInfo(exitOnError=True)
    headBlockNum=min(int(info0["head_block_num"]),int(info1["head_block_num"]))
    libNum=min(int(info0["last_irreversible_block_num"]), int(info1["last_irreversible_block_num"]))
    return (headBlockNum, libNum)



args = TestHelper.parse_args({"--prod-count","--dump-error-details","--keep-logs","-v","--leave-running","--clean-run",
                              "--wallet-port"})
Utils.Debug=args.v
totalProducerNodes=2
totalNonProducerNodes=1
totalNodes=totalProducerNodes+totalNonProducerNodes
maxActiveProducers=21
totalProducers=maxActiveProducers
cluster=Cluster(walletd=True)
dumpErrorDetails=args.dump_error_details
keepLogs=args.keep_logs
dontKill=args.leave_running
prodCount=args.prod_count
killAll=args.clean_run
walletPort=args.wallet_port

walletMgr=WalletMgr(True, port=walletPort)
testSuccessful=False
killEosInstances=not dontKill
killWallet=not dontKill

WalletdName=Utils.EosWalletName
ClientName="cleos"

def get(dict, key):
    assert key in dict, Print("ERROR: could not find key: %s in %s" % (key, json.dumps(dict, indent=4, sort_keys=True)))
    return dict[key]

filterInfoParams = ['head_block_time', 'head_block_num', 'head_block_producer', 'last_irreversible_block_num', 'fork_db_head_block_num', 'head_block_id', 'last_irreversible_block_id', 'fork_db_head_block_id']
def infoDesc(block):
    return { k:v for (k,v) in block.items() if k in filterInfoParams}

try:
    TestHelper.printSystemInfo("BEGIN")

    cluster.setWalletMgr(walletMgr)
    cluster.killall(allInstances=killAll)
    cluster.cleanup()
    Print("Stand up cluster")
    specificExtraNodeosArgs={}
    # producer nodes will be mapped to 0 through totalProducerNodes-1, so the number totalProducerNodes will be the non-producing node
    specificExtraNodeosArgs[totalProducerNodes]="--plugin eosio::test_control_api_plugin"


    # ***   setup topogrophy   ***

    # "bridge" shape connects defprocera through defproducerk (in node0) to each other and defproducerl through defproduceru (in node01)
    # and the only connection between those 2 groups is through the bridge node

    if cluster.launch(prodCount=prodCount, topo="bridge", pnodes=totalProducerNodes,
                      totalNodes=totalNodes, totalProducers=totalProducers,
                      useBiosBootFile=False, specificExtraNodeosArgs=specificExtraNodeosArgs) is False:
        Utils.cmdError("launcher")
        Utils.errorExit("Failed to stand up eos cluster.")
    Print("Validating system accounts after bootstrap")
    cluster.validateAccounts(None)


    # ***   create accounts to vote in desired producers   ***

    accounts=cluster.createAccountKeys(5)
    if accounts is None:
        Utils.errorExit("FAILURE - create keys")
    accounts[0].name="tester111111"
    accounts[1].name="tester222222"
    accounts[2].name="tester333333"
    accounts[3].name="tester444444"
    accounts[4].name="tester555555"

    testWalletName="test"

    Print("Creating wallet \"%s\"." % (testWalletName))
    testWallet=walletMgr.create(testWalletName, [cluster.eosioAccount,accounts[0],accounts[1],accounts[2],accounts[3],accounts[4]])

    for _, account in cluster.defProducerAccounts.items():
        walletMgr.importKey(account, testWallet, ignoreDupKeyWarning=True)

    Print("Wallet \"%s\" password=%s." % (testWalletName, testWallet.password.encode("utf-8")))


    # ***   identify each node (producers and non-producing node)   ***

    nonProdNode=None
    prodNodes=[]
    producers=[]
    for i in range(0, totalNodes):
        node=cluster.getNode(i)
        node.producers=Cluster.parseProducers(i)
        numProducers=len(node.producers)
        Print("node has producers=%s" % (node.producers))
        if numProducers==0:
            if nonProdNode is None:
                nonProdNode=node
                nonProdNode.nodeNum=i
            else:
                Utils.errorExit("More than one non-producing nodes")
        else:
            for prod in node.producers:
                trans=node.regproducer(cluster.defProducerAccounts[prod], "http::/mysite.com", 0, waitForTransBlock=False, exitOnError=True)

            prodNodes.append(node)
            producers.extend(node.producers)


    # ***   delegate bandwidth to accounts   ***

    node=prodNodes[0]
    # create accounts via eosio as otherwise a bid is needed
    for account in accounts:
        Print("Create new account %s via %s" % (account.name, cluster.eosioAccount.name))
        trans=node.createInitializeAccount(account, cluster.eosioAccount, stakedDeposit=0, waitForTransBlock=True, stakeNet=1000, stakeCPU=1000, buyRAM=1000, exitOnError=True)
        transferAmount="100000000.0000 {0}".format(CORE_SYMBOL)
        Print("Transfer funds %s from account %s to %s" % (transferAmount, cluster.eosioAccount.name, account.name))
        node.transferFunds(cluster.eosioAccount, account, transferAmount, "test transfer", waitForTransBlock=True)
        trans=node.delegatebw(account, 20000000.0000, 20000000.0000, waitForTransBlock=True, exitOnError=True)


    # ***   vote using accounts   ***

    #verify nodes are in sync and advancing
    cluster.waitOnClusterSync(blockAdvancing=5)
    index=0
    for account in accounts:
        Print("Vote for producers=%s" % (producers))
        trans=prodNodes[index % len(prodNodes)].vote(account, producers, waitForTransBlock=True)
        index+=1


    # ***   Identify a block where production is stable   ***

    #verify nodes are in sync and advancing
    cluster.waitOnClusterSync(blockAdvancing=5)
    blockNum=node.getNextCleanProductionCycle(trans)
    blockProducer=node.getBlockProducerByNum(blockNum)
    Print("Validating blockNum=%s, producer=%s" % (blockNum, blockProducer))
    cluster.biosNode.kill(signal.SIGTERM)

    #advance to the next block of 12
    currentBlockProducer=blockProducer
    while blockProducer==currentBlockProducer:
        blockNum+=1
        blockProducer=node.getBlockProducerByNum(blockNum)


    # ***   Identify what the production cycle is   ***

    productionCycle=[]
    producerToSlot={}
    prodWindow=-1
    inRowCountPerProducer=12
    previousTimestamp=None
    while True:
        if blockProducer not in producers:
            Utils.errorExit("Producer %s was not one of the voted on producers" % blockProducer)

        productionCycle.append(blockProducer)
        prodWindow+=1
        if blockProducer in producerToSlot:
            Utils.errorExit("Producer %s was first seen in production window %d, but is repeated in production window %d" % (blockProducer, producerToSlot[blockProducer], prodWindow))

        producerToSlot[blockProducer]={"prodWindow":prodWindow, "count":0}
        currentBlockProducer=blockProducer
        while blockProducer==currentBlockProducer:
            producerToSlot[blockProducer]["count"]+=1
            blockNum+=1
            block=node.getBlock(blockNum, exitOnError=True, waitForBlock=True, timeout=None)
            blockProducer=get(block,"producer")
            timestampStr=get(block,"timestamp")
            timestamp=datetime.strptime(timestampStr, Utils.TimeFmt)
            timeDelta=timestamp - previousTimestamp if previousTimestamp is not None else timedelta(milliseconds=500)
            slotChange=int(2 * timeDelta.total_seconds())
            assert slotChange == 1, Print("ERROR: Block number %d skipped from time %s to %s (missed %d slots)" %
                                          (blockNum, previousTimestamp.strftime(Utils.TimeFmt), timestamp.strftime(Utils.TimeFmt), slotChange - 1))
            previousTimestamp=timestamp

        if producerToSlot[currentBlockProducer]["count"]!=inRowCountPerProducer:
            Utils.errorExit("Producer %s, in production window %d, expected to produce %d blocks but produced %d blocks.  At block number %d." %
                            (blockProducer, prodWindow, inRowCountPerProducer, producerToSlot[currentBlockProducer]["count"], blockNum))

        if blockProducer==productionCycle[0]:
            break

    output=None
    for blockProducer in productionCycle:
        if output is None:
            output=""
        else:
            output+=", "
        output+=blockProducer+":"+str(producerToSlot[blockProducer]["count"])
    Print("ProductionCycle ->> {\n%s\n}" % output)

    #retrieve the info for all the nodes to report the status for each
    for node in cluster.getNodes():
        node.getInfo()
    cluster.reportStatus()


    # ***   Killing the "bridge" node   ***

    Print("Sending command to kill \"bridge\" node to separate the 2 producer groups.")
    # block number to start expecting node killed after
    preKillBlockNum=nonProdNode.getBlockNum()
    preKillBlockProducer=nonProdNode.getBlockProducerByNum(preKillBlockNum)
    # kill at last block before defproducerl, since the block it is killed on will get propagated
    killAtProducer="defproducerk"
    nonProdNode.killNodeOnProducer(producer=killAtProducer, whereInSequence=(inRowCountPerProducer-1))
    killTime = datetime.utcnow()

    node0BlocksPerWindow=int((maxActiveProducers + 1)/2)*inRowCountPerProducer
    node1BlocksPerWindow=int((maxActiveProducers - 1)/2)*inRowCountPerProducer
    node1BlocksPerWindowSeconds=node1BlocksPerWindow/2

    # ***   Identify a highest block number to check while we are trying to identify where the divergence will occur   ***

    def blocktime(block):
        timestampStr=block["timestamp"]
        return datetime.strptime(timestampStr, Utils.TimeFmt)


    # will search full cycle after the current block, since we don't know how many blocks were produced since retrieving
    # block number and issuing kill command
    postKillBlockNum=prodNodes[1].getBlockNum()
    lastBlockNum=max([preKillBlockNum,postKillBlockNum])+2*maxActiveProducers*inRowCountPerProducer
    actualLastBlockNum=None
    prodChanged=False
    nextProdChange=False
    #identify the earliest LIB to start identify the earliest block to check if divergent branches eventually reach concensus
    (headBlockNum, libNumAroundDivergence)=getMinHeadAndLib(prodNodes)
    Print("Analyzing block producers from %d till divergence or %d. Head block is %d and lowest LIB is %d" % (preKillBlockNum, lastBlockNum, headBlockNum, libNumAroundDivergence))
    transitionCount=0
    missedTransitionBlock=None
    divergentBlockNode1=None
    divergentTime=None

    def sinceDivergence():
        assert divergentTime is not None
        return (datetime.utcnow() - divergentTime).total_seconds()

    for blockNum in range(preKillBlockNum,lastBlockNum):
        #avoiding getting LIB until my current block passes the head from the last time I checked

        # track the block number and producer from each producing node
        blockNode0=prodNodes[0].getBlock(blockNum, waitForBlock=False)
        blockProducer0=get(blockNode0,"producer") if blockNode0 else None
        blockNode1=prodNodes[1].getBlock(blockNum, waitForBlock=True, timeout=1)
        assert blockNode1 is not None, Utils.Print("ERROR: block number %d should be available for node 01." % (blockNum))
        blockProducer1=get(blockNode1,"producer") if blockNode1 else None

        #in the case that the preKillBlockNum was also produced by killAtProducer, ensure that we have
        #at least one producer transition before checking for killAtProducer
        if not prodChanged:
            if blockProducer0 is not None and preKillBlockProducer!=blockProducer0:
                prodChanged=True

        #since it is killing for the last block of killAtProducer, we look for the next producer change
        if not nextProdChange and prodChanged and blockProducer1==killAtProducer:
            nextProdChange=True
        elif nextProdChange and blockProducer1!=killAtProducer:
            nextProdChange=False
            if blockProducer0 is None:
                info0=prodNodes[0].getInfo()
                info1=prodNodes[1].getInfo()
                actualLastBlockNum=blockNum
                divergentBlockNode1=blockNode1
                divergentTime=blocktime(blockNode1)
                Print("Divergence identified at block %s, node_00 producer: %s, node_01 producer: %s, "
                      "delay from kill sent to divergent block: %.3f sec, time since divergence: %.1f sec, "
                      "node_00 info: %s, node_01 info: %s" %
                      (blockNum, blockProducer0, blockProducer1, (divergentTime - killTime).total_seconds(),
                      sinceDivergence(), info0, info1))
                break
            else:
                assert blockProducer0 == blockProducer1,\
                    Utils.Print("ERROR: Block producers for block num %d is %s for node_00 and is %s for node_01.  " +
                                "Either the timing for this test needs to be fixed (if timestamp from kill message " +
                                "till now exceeds %d seconds) or there is another issue.  " +
                                "From node 00: %s, from node 00: %s" %
                                (blockNum, blockProducer0, blockProducer1, node1BlocksPerWindowSeconds, blockNode0, blockNode1))
                missedTransitionBlock=blockNum
                transitionCount+=1
                # allow this to transition twice, in case the script was identifying an earlier transition than the bridge node received the kill command
                if transitionCount>1:
                    Print("At block %d and have passed producer: %s %d times and we have not diverged, stopping looking and letting errors report" % (blockNum, killAtProducer, transitionCount))
                    actualLastBlockNum=blockNum
                    break

        #if we diverge before identifying the actualLastBlockNum, then there is an ERROR
        if blockProducer0!=blockProducer1:
            extra="" if transitionCount==0 else " Diverged after expected killAtProducer transition at block %d." % (missedTransitionBlock)
            Utils.errorExit("Groups reported different block producers for block number %d.%s %s != %s." % (blockNum,extra,blockProducer0,blockProducer1))

    #verify that the non producing node is not alive (and populate the producer nodes with current getInfo data to report if
    #an error occurs)
    if nonProdNode.verifyAlive():
        Utils.errorExit("Expected the non-producing node to have shutdown.")

    libNumAroundDivergence=get(prodNodes[0].getInfo(),"last_irreversible_block_num")

    for prodNode in prodNodes:
        info=prodNode.getInfo()
        Print("After analyzing for divergence, node info: %s" % (info))

    killBlockNum=actualLastBlockNum
    lastBlockNum=killBlockNum+node1BlocksPerWindow  # go till the 1st testnet is exactly the same size as the 2nd

    info0=prodNodes[0].getInfo()
    info1=prodNodes[1].getInfo()
    Print("Tracking the blocks from the divergence till there are 10*12 blocks on one chain and 10*12+1 on the other, from block %d to %d. Time since divergence: %.1f. node_00 info: %s, node_01 info: %s" % (killBlockNum, lastBlockNum, sinceDivergence(), info0, info1))

    blockProducers0=[]
    blockProducers1=[]

    blockNode0=prodNodes[0].getBlock(killBlockNum - 1, exitOnError=True, silentErrors=True, waitForBlock=True, timeout=None)
    blockNode1=prodNodes[1].getBlock(killBlockNum - 1, exitOnError=True, silentErrors=True, waitForBlock=True, timeout=None)
    Print("blockNum: %d, prod0: %s, t0: %s, prod1: %s, t1: %s, <--start" % (killBlockNum - 1, blockNode0["producer"], blockNode0["timestamp"], blockNode1["producer"], blockNode1["timestamp"]))

    previousNode0Timestamp=blocktime(blockNode0)
    previousNode1Timestamp=blocktime(blockNode1)

    previousBlockNode0=None
    previousBlockNode1=None

    timeout0 = int((datetime.utcnow() - killTime).total_seconds() + 0.5)
    for blockNum in range(killBlockNum,lastBlockNum):
        blockNode0=prodNodes[0].getBlock(blockNum, exitOnError=True, silentErrors=True, waitForBlock=True, timeout=timeout0)
        # only the first block will not be ready, the rest should always be available in less than 1 second
        timeout0=1
        blockProducer0=blockNode0["producer"]

        blockNode1=prodNodes[1].getBlock(blockNum, exitOnError=True, silentErrors=True, waitForBlock=True, timeout=timeout0)
        blockProducer1=blockNode1["producer"]
        blockProducers0.append({"blockNum":blockNum, "prod":blockProducer0})
        blockProducers1.append({"blockNum":blockNum, "prod":blockProducer1})

        Print("blockNum: %d, prod0: %s, t0: %s, t1: %s, prod1: %s, t0: %s, t1: %s" % (blockNum, blockProducer0, previousNode0Timestamp.strftime(Utils.TimeFmt), blockNode0["timestamp"], blockProducer1, previousNode1Timestamp.strftime(Utils.TimeFmt), blockNode1["timestamp"]))
        node0Timestamp=blocktime(blockNode0)
        timeDeltaNode0=node0Timestamp - previousNode0Timestamp
        slotChange=int(2 * timeDeltaNode0.total_seconds())

        if blockNum == killBlockNum:
            assert slotChange == node1BlocksPerWindow + 1, Print("ERROR: Skipped %d slots for block number %d, should have skipped %d because of divergence in the branches. t0: %s, t1: %s - Node0 prev block: %s, this block: %s, Node 1 prev block: %s, this block: %s" % (slotChange - 1, blockNum, node1BlocksPerWindow, previousNode0Timestamp.strftime(Utils.TimeFmt), node0Timestamp.strftime(Utils.TimeFmt), previousBlockNode0, blockNode0, previousBlockNode1, blockNode1))
        else:
            assert slotChange == 1, Print("ERROR: Skipped %d slots for block number %d, should not skip other than where the branches diverge. t0: %s, t1: %s - Node0 prev block: %s, this block: %s, Node 1 prev block: %s, this block: %s" % (slotChange - 1, blockNum, previousNode0Timestamp.strftime(Utils.TimeFmt), node0Timestamp.strftime(Utils.TimeFmt), previousBlockNode0, blockNode0, previousBlockNode1, blockNode1))

        previousNode0Timestamp=node0Timestamp

        node1Timestamp=blocktime(blockNode1)
        timeDeltaNode1=node1Timestamp - previousNode1Timestamp
        slotChange=int(2 * timeDeltaNode1.total_seconds())
        assert slotChange == 1, Print("ERROR: Skipped %d slots for block number %d, should not skip prior to divergence in the branches. t0: %s, t1: %s - Node0 prev block: %s, this block: %s, Node 1 prev block: %s, this block: %s" % (slotChange - 1, blockNum, previousNode0Timestamp.strftime(Utils.TimeFmt), node1Timestamp.strftime(Utils.TimeFmt), previousBlockNode0, blockNode0, previousBlockNode1, blockNode1))
        previousNode1Timestamp=node1Timestamp

        previousBlockNode0=blockNode0
        previousBlockNode1=blockNode1

    blockNode0=prodNodes[0].getBlock(lastBlockNum + 1, exitOnError=True, silentErrors=True, waitForBlock=True, timeout=1)
    assert blockNode0 is not None,\
        Print("ERROR: Failed to retrieve block number %d from Node_00 after waiting 1 second, even though it has the "
              "next producer: %s. node_00 info: %s, node_01 info: %s" % (blockProducers0[-1]["prod"], prodNodes[0].getInfo(), prodNodes[1].getInfo()))

    info0=prodNodes[0].getInfo()
    info1=prodNodes[1].getInfo()
    Print("Relaunching the non-producing bridge node to connect the producing nodes again. Time since divergence: %.1f. node_00 info: %s, node_01 info: %s" % (sinceDivergence(), info0, info1))

    if not nonProdNode.relaunch(nonProdNode.nodeNum, None):
        Utils.errorExit("Failure - (non-production) node %d should have restarted" % (nonProdNode.nodeNum))

    info0=prodNodes[0].getInfo()
    info1=prodNodes[1].getInfo()
    Print("Analyzing the cached producers from the divergence to the lastBlockNum and verify they stay diverged, expecting divergence at block %d, while allowing network to resync. Time since divergence: %.1f. node_00 info: %s, node_01 info: %s" % (killBlockNum, sinceDivergence(), info0, info1))

    firstDivergence=analyzeBPs(blockProducers0, blockProducers1, expectDivergence=True)
    if firstDivergence!=killBlockNum:
        Utils.errorExit("Expected to diverge at %s, but diverged at %s." % (firstDivergence, killBlockNum))
    blockProducers0=[]
    blockProducers1=[]

    for prodNode in prodNodes:
        info=prodNode.getInfo()
        Print("Before relaunch, node info: %s" % (info))

    Print("Waiting to allow forks to resolve. Time since divergence: %.1f." % (sinceDivergence()))

    for prodNode in prodNodes:
        info=prodNode.getInfo()
        Print("Waiting to resolve fork, node info: %s" % (info))

    # ensure that the nodes have enough time to get in concensus, so wait for 3 producers to produce their complete round
    time.sleep(inRowCountPerProducer * 3 / 2)
    remainingChecks=20
    match=False
    checkHead=False
    while remainingChecks>0:
        checkMatchBlock=killBlockNum if not checkHead else prodNodes[0].getBlockNum()
        blockProducer0=prodNodes[0].getBlockProducerByNum(checkMatchBlock)
        blockProducer1=prodNodes[1].getBlockProducerByNum(checkMatchBlock)
        match=blockProducer0==blockProducer1
        if match:
            if checkHead:
                break
            else:
                checkHead=True
                continue
        Print("Fork has not resolved yet, wait a little more. Time since divergence: %.1f. Block %s has producer %s for node_00 and %s for node_01.  Original divergence was at block %s. Wait time remaining: %d" % (sinceDivergence(), checkMatchBlock, blockProducer0, blockProducer1, killBlockNum, remainingChecks))
        time.sleep(1)
        remainingChecks-=1

    for prodNode in prodNodes:
        info=prodNode.getInfo()
        Print("Fork resolved, node info: %s" % (info))

    # ensure all blocks from the lib before divergence till the current head are now in consensus
    endBlockNum=max(prodNodes[0].getBlockNum(), prodNodes[1].getBlockNum())

    Print("Identifying the producers from the saved LIB to the current highest head, from block %d to %d. Time since divergence: %.1f." % (libNumAroundDivergence, endBlockNum, sinceDivergence()))

    blockNode0=prodNodes[0].getBlock(libNumAroundDivergence - 1, exitOnError=True, silentErrors=True, waitForBlock=True, timeout=None)
    timestampStr=get(blockNode0, "timestamp")
    previousTimestamp=datetime.strptime(timestampStr, Utils.TimeFmt)
    Print("blockNum: %d, t0: %s" % (get(blockNode0, "block_num"), previousTimestamp.strftime(Utils.TimeFmt)))
    timestamps=[]
    for blockNum in range(libNumAroundDivergence,endBlockNum):
        blockNode0=prodNodes[0].getBlock(blockNum, exitOnError=True, silentErrors=True)
        blockProducer0=blockNode0["producer"]
        blockNode1=prodNodes[1].getBlock(blockNum, exitOnError=True, silentErrors=True)
        blockProducer1=blockNode1["producer"]
        blockProducers0.append({"blockNum":blockNum, "prod":blockProducer0})
        blockProducers1.append({"blockNum":blockNum, "prod":blockProducer1})

        # report slot errors after we have determined if there are any divergences
        timestampStr=blockNode0["timestamp"]
        timestamp=datetime.strptime(timestampStr, Utils.TimeFmt)
        timeDelta=timestamp - previousTimestamp
        slotChange=int(2 * timeDelta.total_seconds())
        Print("block0 - blockNum: %d, slotChange: %d, t0: %s, t1: %s, producer: %s" % (blockNode0["block_num"], slotChange, previousTimestamp.strftime(Utils.TimeFmt), timestamp.strftime(Utils.TimeFmt), blockProducer0))
        timestamps.append({"blockNum":blockNode0["block_num"], "slotChange":slotChange, "producer":blockProducer0, "t0":previousTimestamp.strftime(Utils.TimeFmt), "t1":timestamp.strftime(Utils.TimeFmt)})

        timestampStr=blockNode1["timestamp"]
        timestamp=datetime.strptime(timestampStr, Utils.TimeFmt)
        timeDelta=timestamp - previousTimestamp
        slotChange=int(2 * timeDelta.total_seconds())
        Print("block1 - blockNum: %d, slotChange: %d, t0: %s, t1: %s, producer: %s" % (blockNode1["block_num"], slotChange, previousTimestamp.strftime(Utils.TimeFmt), timestamp.strftime(Utils.TimeFmt), blockProducer1))
        previousTimestamp=timestamp


    Print("Analyzing the producers from the saved LIB to the current highest head and verify they match now. Time since divergence: %.1f." % (sinceDivergence()))

    analyzeBPs(blockProducers0, blockProducers1, expectDivergence=False)

    resolvedKillBlockProducer=None
    for prod in blockProducers0:
        if prod["blockNum"]==killBlockNum:
            resolvedKillBlockProducer = prod["prod"]
    if resolvedKillBlockProducer is None:
        Utils.errorExit("Did not find find block %s (the original divergent block) in blockProducers0, test setup is wrong.  blockProducers0: %s" % (killBlockNum, ", ".join(blockProducers)))
    Print("Fork resolved and determined producer %s for block %s. Time since divergence: %.1f." % (resolvedKillBlockProducer, killBlockNum, sinceDivergence()))

    node0WindowSkipFound=False
    for timestamp in timestamps:
        slotChange=timestamp["slotChange"]
        # allow for one skip in slots, which has to occur after all the successive blocks from node1, and before node1 would start producing again
        if not node0WindowSkipFound and slotChange > node1BlocksPerWindow and slotChange <= node0BlocksPerWindow:
            node0WindowSkipFound=True
            continue

#        assert slotChange == 1, Print("ERROR: Block Number %d skipped %d block slots. t0: %s, t1: %s" % (timestamp["blockNum"], timestamp["slotChange"] - 1, timestamp["t0"], timestamp["t1"]))
        assert slotChange <= 2, Print("ERROR: Block Number %d skipped %d block slots. t0: %s, t1: %s. node0WindowSkipFound: %s, node1BlocksPerWindow: %s, node0BlocksPerWindow: %s" %
                                      (timestamp["blockNum"], slotChange - 1, timestamp["t0"], timestamp["t1"], node0WindowSkipFound, node1BlocksPerWindow, node0BlocksPerWindow))
        if slotChange == 2:
            Print("ERROR: Block Number %d skipped %d block slots. t0: %s, t1: %s. node0WindowSkipFound: %s" %
                    (timestamp["blockNum"], slotChange - 1, timestamp["t0"], timestamp["t1"], node0WindowSkipFound))

    blockProducers0=[]
    blockProducers1=[]

    testSuccessful=True
finally:
    TestHelper.shutdown(cluster, walletMgr, testSuccessful=testSuccessful, killEosInstances=killEosInstances, killWallet=killWallet, keepLogs=keepLogs, cleanRun=killAll, dumpErrorDetails=dumpErrorDetails)

    if not testSuccessful:
        Print(Utils.FileDivider)
        Print("Compare Blocklog")
        cluster.compareBlockLogs()
        Print(Utils.FileDivider)
        Print("Print Blocklog")
        cluster.printBlockLog()
        Print(Utils.FileDivider)

exit(0)
