#!/bin/bash

ps -ef | grep -v grep | grep uwsgi | grep app.ini | awk '{print $2}' | xargs kill -9

export PATH="/storage/ysma/anaconda2/bin:$PATH"
uwsgi -i app.ini 2>&1 >> log.txt & 
