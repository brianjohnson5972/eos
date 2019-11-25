#!/usr/bin/env python3

from testUtils import Utils
from testUtils import NodeosLogBlockReader

from TestHelper import AppArgs
from TestHelper import TestHelper

import os

appArgs = AppArgs()
minTotalAccounts = 20
extraArgs = appArgs.add(flag="--source", type=str, help="Source file or directory to read in.")
extraArgs = appArgs.add(flag="--output", type=str, help="Directory to write output to.", default=".")
extraArgs = appArgs.add_bool(flag="--fork-analysis", help="Indicate if fork analysis files should be created.")
args = TestHelper.parse_args({"-v"}, applicationSpecificArgs=appArgs)

Utils.Debug=args.v

assert args.source is not None, Utils.Print("ERROR: source must be provided")
assert os.path.exists(args.source), Utils.Print("ERROR: source: %s doesn't exists" % (args.source))
assert os.path.exists(args.output), Utils.Print("ERROR: output: %s doesn't exists" % (args.output))
files = None
if os.path.isdir(args.source):
    for _, _, tempFiles in os.walk(args.source):
        files = tempFiles
else:
    files = [ args.source]

blocks = []
for file in files:
    _, filename = os.path.split(file)
    filename, ext = os.path.splitext(filename)
    outFilename = os.path.join(args.output, filename + "-blocks" + ext)
    forkFilename = os.path.join(args.output, filename + "-forks" + ext) if args.fork_analysis else None
    blockReader = NodeosLogBlockReader(file, forkAnalysisFilename=forkFilename)
    outFile = open(outFilename, "w")
    blockCount = 0
    while True:
        block = blockReader.next()
        if block is None:
            if Utils.Debug: Utils.Print("Finished file: %s, read %d blocks" % (filename, blockCount))
            break
        blockCount += 1
        outFile.write(str(block) + '\n')
    outFile.close()