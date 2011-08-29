
# MapReduce algorithm for graph evaluation implemented on Hadoop platform

This repo contains codes used to evaluate graph performance on Hadoop platform. Since we have created huge image graph for large-scale image collection, the evaluation of such huge image graph is a challenge. Traditionally, the evaluation is performed sequentially by examining neighboring nodes for each node in the graph. It follows the approach of evaluating ranking list in information retrieval. The performance is represented by MAP (mean average precision). However, this process will be time-consuming when dealing with millions of nodes. MapReduce provides an useful tool to evaluate the graph in parallel way.

# Usage:

make       # compile the codes

make clean # clean output directories on HDFS

make run   # run the evaluation program on Hadoop

