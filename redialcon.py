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
step = 60
metric = 'redis'
#send data when error happened
alwaysSend = True
defaultDataWhenFailed = -1
host = '127.0.0.1'
port = '6379'
user = 'redis'
pswd = ''
endPoint = socket.gethostname()
redisCli = '/usr/bin/redis-cli'
print '\n'

class RedisStats:
    # 如果你是自己编译部署到redis，请将下面的值替换为你到redis-cli路径
    _stat_regex = re.compile(ur'(\w+):([0-9]+\.?[0-9]*)\r')

    def __init__(self):
        global redisCli,host,port,pswd
        self._cmd = '%s -h %s -p %s info' % (redisCli, host, port)
        if pswd not in ['', None]:
            self._cmd = "%s -a %s" % (self._cmd, passwd )
    def stats(self):
        ' Return a dict containing redis stats '
        if(debug):
            print self._cmd
        info = commands.getoutput(self._cmd)
        if(info.find('No such file or directory') != -1):
            print '[redialcon][error]could not find redis-cli , please assign full redis-cli path with -b , e.g. /usr/bin/redis-cli'
            sys.exit(2)
        dic = dict(self._stat_regex.findall(info))
        if(len(dic) == 0):
            print '[redialcon][error]could not get redis info , empty '
            sys.exit(2)
        return dic

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
    print '    -b redis-cli path , default /usr/bin/redis-cli'
    sys.exit(2)

def main():
    global debug,timestamp,falconAgentUrl,step,metric,alwaysSend,defaultDataWhenFailed,host,port,user,pswd,endPoint
    
    if len(sys.argv[1:]) == 0:
        usage()

    try:
        opts, args = getopt.getopt(sys.argv[1:],"t:f:a:v:h:u:p:e:c:D:H:b:",['--help'])
    except getopt.GetoptError:
        usage()

    global debug,timestamp,falconAgentUrl,step,metric,alwaysSend,defaultDataWhenFailed,host,port,user,pswd,redisCli
    for opt, arg in opts:
        if opt in ('-H','--help'):
            usage()
        if opt == '-t':
            try:
                step = int(arg)
            except Exception:
                print e
                sys.exit(2)
        if opt == '-D':
            debug = arg.lower() == 'true' and True or False
        elif opt == '-f':
            falconAgentUrl = arg
        elif opt == '-m':
            metric = arg
        elif opt == '-a':
            alwaysSend = arg.lower() == 'true' and True or False
        elif opt == '-e':
            endPoint = arg
        elif opt == '-b':
            redisCli = arg
        elif opt == '-v':
            try:
                defaultDataWhenFailed = int(arg)
            except Exception:
                print e
                sys.exit(2)
        elif opt == '-h':
            if arg.find(":") == -1:
                print '[redialcon][error]illegel param -h %s , should be host:port' % (arg)
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
                print '[redialcon][error]error parse config file :%s' % (arg)
                sys.exit(2)

    print '[redialcon]%s@%s:%s %s %s' %(user,host,port,falconAgentUrl,metric)
    
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
  
    dataToPush = []   
    tags = 'port=%s' % port
    value = None

    conn = RedisStats()
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
        if(value == None):
            value = defaultDataWhenFailed

        i = {
            'Metric': '%s.%s' % (metric, key),
            'Endpoint': endPoint,
            'Timestamp': timestamp,
            'Step': step,
            'Value': value,
            'CounterType': vtype,
            'TAGS': tags
        }
        dataToPush.append(i)

    if(debug):
        print json.dumps(dataToPush, sort_keys=True,indent=4)
    method = "POST"
    handler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(handler)
    request = urllib2.Request(falconAgentUrl, data=json.dumps(dataToPush) )
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
