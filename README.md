elasticbrainsurgeon
===================

Elasticsearch Brain Surgeon

Detects a split brain condition in an elastic search cluster, and acts on the
violating host, if it does not agree with the majority of nodes within the
cluster as to who the master is.

Depends on Python Elasticsearch Library (install with pip install elasticsearch).
Docs for the Elasticsearch Library are found here: http://elasticsearch-py.readthedocs.org/en/master/
