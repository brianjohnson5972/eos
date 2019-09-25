#!/usr/bin/env python3

from testUtils import Utils
from Cluster import Cluster
from WalletMgr import WalletMgr
from Node import Node
from Node import ReturnType
from TestHelper import TestHelper

import decimal
import re

###############################################################
# nodeos_run_test
#
# General test that tests a wide range of general use actions around nodeos and keosd
#
###############################################################

Print=Utils.Print
errorExit=Utils.errorExit
cmdError=Utils.cmdError
from core_symbol import CORE_SYMBOL

args = TestHelper.parse_args({"--host","--port","--prod-count","--defproducera_prvt_key","--defproducerb_prvt_key","--mongodb"
                              ,"--dump-error-details","--dont-launch","--keep-logs","-v","--leave-running","--only-bios","--clean-run"
                              ,"--sanity-test","--wallet-port"})
server=TestHelper.LOCAL_HOST
port=TestHelper.DEFAULT_PORT
debug=1
enableMongo=0
dumpErrorDetails=True
keepLogs=True
dontLaunch=0
dontKill=True
prodCount=1
onlyBios=0
killAll=True
sanityTest=0
walletPort=TestHelper.DEFAULT_WALLET_PORT

Utils.Debug=debug
localTest=True if server == TestHelper.LOCAL_HOST else False
cluster=Cluster(host=server, port=port, walletd=True, enableMongo=enableMongo)
walletMgr=WalletMgr(True, port=walletPort)
testSuccessful=False
killEosInstances=not dontKill
killWallet=not dontKill

WalletdName=Utils.EosWalletName
ClientName="cleos"
timeout = .5 * 12 * 2 + 60 # time for finalization with 1 producer + 60 seconds padding
Utils.setIrreversibleTimeout(timeout)

try:
    TestHelper.printSystemInfo("BEGIN")
    cluster.setWalletMgr(walletMgr)
    Print("SERVER: %s" % (server))
    Print("PORT: %d" % (port))

    cluster.killall(allInstances=killAll)
    cluster.cleanup()
    Print("Stand up cluster")
    if cluster.launch(pnodes=1, totalNodes=3) is False:
        cmdError("launcher")
        errorExit("Failed to stand up eos cluster.")

    Print("Validating system accounts after bootstrap")
    cluster.validateAccounts(None)

    testSuccessful=True
finally:
    TestHelper.shutdown(cluster, walletMgr, testSuccessful, killEosInstances, killWallet, keepLogs, killAll, dumpErrorDetails)

exit(0)
