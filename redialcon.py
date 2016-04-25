#!/bin/env python
#-*- coding:utf-8 -*-

__author__ = 'diego.zhu'
__email__ = 'zhuhaiyang55@gmail.com'

import getopt
import psycopg2
import psycopg2.extras
import traceback
import json
import time
import socket
import os
import re
import sys
import commands
import urllib2, base64

debug = False
timestamp = int(time.time())
falconAgentUrl = 'http://127.0.0.1:1988/v1/push'
Step = 60
Metric = 'postgresql'
#send data when error happened
alwaysSend = True
defaultDataWhenFailed = -1
host = '127.0.0.1'
port = '6379'
user = 'redis'
pswd = ''
endPoint = socket.gethostname()

class RedisStats:
    # 如果你是自己编译部署到redis，请将下面的值替换为你到redis-cli路径
    _redis_cli = '/usr/local/bin/redis-cli'
    _stat_regex = re.compile(ur'(\w+):([0-9]+\.?[0-9]*)\r')

    def __init__(self,  port='6379', passwd=None, host='127.0.0.1',user='redis'):
        self._cmd = '%s -h %s -p %s info' % (self._redis_cli, host, port)
        if passwd not in ['', None]:
            self._cmd = "%s -a %s" % (self._cmd, passwd )
        print slef._cmd
    def stats(self):
        ' Return a dict containing redis stats '
        print 'aa'
        print slef._cmd
        info = commands.getoutput(self._cmd)
        return dict(self._stat_regex.findall(info))

def usage():
    print ''
    print 'python redialcon.py [options]'
    print ''
    print '    -a always send if failed collectting data , kind of falcon nodata , default True'
    print '    -D debug , default False'
    print '    -v default data when failed , default -1'
    print '    -t time-interval , default 60 in second'
    print '    -f falcon-agent-push-url , default http://127.0.0.1:1988/v1/push'
    print '    -m metric , default postgresql'
    print '    -h host:port , default 127.0.0.1:5432'
    print '    -u user , default redis'
    print '    -p password , default empty'
    print '    -e end point , default hostname'
    print '    -c redis config file'
    sys.exit(2)

def main():
    global debug,timestamp,falconAgentUrl,Step,Metric,alwaysSend,defaultDataWhenFailed,host,port,user,pswd
    
    if len(sys.argv[1:]) == 0:
        usage()

    try:
        opts, args = getopt.getopt(sys.argv[1:],"t:f:a:v:d:h:u:p:e:c:D:H:",['--help'])
    except getopt.GetoptError:
        usage()

    global debug,timestamp,falconAgentUrl,Step,Metric,alwaysSend,defaultDataWhenFailed,host,port,user,pswd
    for opt, arg in opts:
        if opt in ('-H','--help'):
            usage()
        if opt == '-t':
            Step = arg
        if opt == '-D':
            debug = arg.lower() == 'true' and True or False
        elif opt == '-f':
            falconAgentUrl = arg
        elif opt == '-m':
            Metric = arg
        elif opt == '-a':
            alwaysSend = arg.lower() == 'true' and True or False
        elif opt == '-e':
            endPoint = arg
        elif opt == '-v':
            defaultDataWhenFailed = arg
        elif opt == '-h':
            if arg.find(":") == -1:
                print 'illegel param -h %s , should be host:port' % (arg)
                sys.exit(2)
            host = arg.split(':')[0]
            port = arg.split(':')[1]
        elif opt == '-u':
            user = arg
        elif opt == '-p':
            pswd = arg
        elif opt == '-c':
            try:
                port = commands.getoutput("sed -n 's/^port *\([0-9]\{4,5\}\)/\\1/p' %s" % arg)
                pswd = commands.getoutput("sed -n 's/^requirepass *\([^ ]*\)/\\1/p' %s" % arg)
            except Exception:
                print 'error parse config file :%s' % (arg)
                sys.exit(2)

    print '%s@%s:%s/%s %s %s' %(user,host,port,db,falconAgentUrl,Metric)
    
    monit_keys = [
        ('connected_clients','GAUGE'), 
        ('blocked_clients','GAUGE'), 
        ('used_memory','GAUGE'),
        ('used_memory_rss','GAUGE'),
        ('mem_fragmentation_ratio','GAUGE'),
        ('total_commands_processed','COUNTER'),
        ('rejected_connections','COUNTER'),
        ('expired_keys','COUNTER'),
        ('evicted_keys','COUNTER'),
        ('keyspace_hits','COUNTER'),
        ('keyspace_misses','COUNTER'),
        ('keyspace_hit_ratio','GAUGE'),
    ]
  
        
    tags = 'port=%s' % port

    conn = RedisStats(port, passwd)
    stats = conn.stats()
    for key,vtype in monit_keys:
        if key == 'keyspace_hit_ratio':
            try:
                value = float(stats['keyspace_hits'])/(int(stats['keyspace_hits']) + int(stats['keyspace_misses']))
            except Exception:
                value = defaultDataWhenFailed
        elif key == 'mem_fragmentation_ratio':
            try:
                value = float(stats[key])
            except Exception:
                value = defaultDataWhenFailed
        
        i = {
            'Metric': '%s.%s' % (metric, key),
            'Endpoint': endPoint,
            'Timestamp': timestamp,
            'Step': Step,
            'Value': value,
            'CounterType': vtype,
            'TAGS': tags
        }
        p.append(i)

    print json.dumps(p, sort_keys=True,indent=4)
    method = "POST"
    handler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(handler)
    request = urllib2.Request(falconAgentUrl, data=json.dumps(p) )
    request.add_header("Content-Type",'application/json')
    request.get_method = lambda: method
    try:
        connection = opener.open(request)
    except urllib2.HTTPError,e:
        connection = e

    # check. Substitute with appropriate HTTP code.
    if connection.code == 200:
        print connection.read()
    else:
        print '{"err":1,"msg":"%s"}' % connection
if __name__ == '__main__':
    proc = commands.getoutput(' ps -ef|grep %s|grep -v grep|wc -l ' % os.path.basename(sys.argv[0]))
    if int(proc) < 5:
        main()
