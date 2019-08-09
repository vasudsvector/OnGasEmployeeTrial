from pycallgraph import PyCallGraph, Config
from pycallgraph.output import GraphvizOutput
from main import RunPred
rp = RunPred('aws_id', 'aws_sec', 'aws_buck', startfromscratch=True, run_until_date='30/06/2015', custids=custids, local_test=True)
with PyCallGraph(output=GraphvizOutput()):
    rp.main()