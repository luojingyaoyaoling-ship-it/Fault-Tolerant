#!/bin/sh
taskset -c $1 python worker/worker_service.py "$2" "$3"
