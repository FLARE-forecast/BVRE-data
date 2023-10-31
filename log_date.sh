#!/bin/bash
while true; do date >> /data/bjorn-logs/time.log; sleep 1; done & sleep 1800; kill $!

