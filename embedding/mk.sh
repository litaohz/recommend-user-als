#!/usr/bin/env bash
kinit -kt ndir.keytab ndir@HADOOP.HZ.NETEASE.COM
hadoop fs -mkdir $1