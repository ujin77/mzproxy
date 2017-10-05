#!/usr/bin/python
#
#pip install py-zabbix
#pip install paho-mqtt
#pip install python-daemon
#pip install lockfile

from zabbix.api import ZabbixAPI
from pyzabbix import ZabbixMetric, ZabbixSender
import paho.mqtt.client as mqtt

import json
import sqlite3
import random
import time, sys
import threading
import daemon, signal
from daemon import pidfile
import argparse
import logging
import logging.handlers
import os
import re


CN_RES = {
    0: "Connection successful",
    1: "Connection refused - incorrect protocol version",
    2: "Connection refused - invalid client identifier",
    3: "Connection refused - server unavailable",
    4: "Connection refused - bad username or password",
    5: "Connection refused - not authorised",
}

        
class cmzProxy(object):
    """docstring for cmzProxy"""

    _memdb=None
    _curdb=None
    _mqtt_client=None
    _thread = None
    _last_update = None
    keyreg = None
    log=None
    zbx_url='http://zabbix.local/'
    zbx_host='zabbix.local'
    zbx_user='mqtt'
    zbx_pwd='mqtt'
    mqtt_broker_host='mqtt.local'
    update_interval = 600
    keypattern = r"^mqtt\[[\'\"]?([\$\/a-zA-Z0-9]+)[\'\"]?\,?(\d)?\]$"

    def __init__(self, debugging=False):
        super(cmzProxy, self).__init__()

        self.keyreg = re.compile(self.keypattern)
        self.log = logging.getLogger('mzproxy')
        self.log.setLevel(logging.INFO)
        if debugging == True:
            self.log.setLevel(logging.DEBUG)
        lh = logging.handlers.SysLogHandler()
        lh.setFormatter(logging.Formatter('%(filename)s[%(process)d]: %(message)s'))
        self.log.addHandler(lh)
        self._thread = None
        self._last_update = time.time()
        self._mqtt_client = mqtt.Client("mzproxy", clean_session=False)
        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_disconnect = self.on_disconnect
        self._mqtt_client.on_subscribe = self.on_subscribe
        self._mqtt_client.on_unsubscribe = self.on_unsubscribe
        self._mqtt_client.on_message = self.on_message
        # self._mqtt_client.on_log = self.on_log
        self._mqtt_client.enable_logger(self.log)
        self.start()

    def init_db(self):
        if self._memdb == None:
            self._memdb = sqlite3.connect(":memory:")
            self._curdb = self._memdb.cursor()
            self._curdb.execute("create table items(z_host, z_key, topic, qos)")
            self._curdb.execute("create table subscriptions(topic)")

    def parse_key(self, key):
        result = self.keyreg.match(key)
        if result:
            return(result.groups(0))
        return(None, None)

    def update_subscribtion(self):
        self.log.debug("Update")
        try:
            _zapi = ZabbixAPI(url=self.zbx_url, user=self.zbx_user, password=self.zbx_pwd)
            list_items = _zapi.item.get(application='mqtt', output=['name','key_'], type=2, filter={'status': 0}, selectHosts=['name','status'])
        except Exception as err:
            self.log.error(err)
            return
        if list_items:
            self._curdb.execute("delete from items")
            for item in list_items:
                if item['hosts'][0]['status']=="0":
                    z_host = item['hosts'][0]['name']
                    z_key = item['key_']
                    (topic, qos) = self.parse_key(z_key)
                    # topic = item['key_'][5:-1].strip("'")
                    self._curdb.execute("insert into items (z_host, z_key, topic, qos) values ( ?, ?, ?, ? )", (z_host, z_key, topic, qos))
        ###
        self._curdb.execute("select topic, qos from items where topic not in (select topic from subscriptions)")
        for row in self._curdb.fetchall():
            topic=row[0]
            qos=row[1]
            self._mqtt_client.subscribe(topic, qos)
            self._curdb.execute("insert into subscriptions (topic) values ( :topic )", { 'topic': topic })
            self.log.info("Subscribe: " + str(topic) + " qos["+ str(qos) +"] for host[" + str(z_host) + "]")
        for row in self._curdb.execute("select topic from subscriptions where topic not in (select topic from items)"):
            topic=row[0]
            self._mqtt_client.unsubscribe(topic)
            self.log.info("Unsubscribe " + str(topic))
        self._curdb.execute("delete from subscriptions where topic not in (select topic from items)")

    def zbxSend(self, z_host, z_key, z_val):
        self.log.debug("Send to zabbix " + z_key + "="+ z_val)
        try:
            result = ZabbixSender(self.zbx_host).send([ZabbixMetric(z_host, z_key, z_val)])
            if result._failed != 0:
                self.log.error("Send to zabbix error")
        except Exception as err:
            self.log.error(err)        

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):
        self.log.warn("MQTT "+ CN_RES[rc] + ". flags: " + str(flags))
        self._mqtt_client.subscribe("$SYS/broker/uptime")
        if rc==0:
            self.update_subscribtion()

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            self.log.warn("Unexpected MQTT disconnection[" + str(rc) + "]. Attempting to reconnect.")
        else:
            self.log.warn("Normal MQTT disconnection.")

    def on_subscribe(self, client, userdata, mid, granted_qos):
        self.log.info("on_subscribe[" + str(mid) + "]")

    def on_unsubscribe(self, client, userdata, mid):
        self.log.info("on_unsubscribe[" + str(mid) + "]")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        # print(msg.topic+": "+str(msg.payload))
        for row in self._curdb.execute("select z_host, z_key from items where topic = :topic", {'topic': msg.topic}):
            self.zbxSend(row[0], row[1], self._transform_message(msg))
        if self._last_update + self.update_interval < time.time():
            self._last_update = time.time()
            self.update_subscribtion()

    def _transform_message(self, msg):
        topic=str(msg.topic)
        payload=str(msg.payload)
        # print(topic + ": "+ payload)
        if topic == '$SYS/broker/uptime':
            # print(topic+": "+ payload.rstrip(' seconds'))
            return (payload.rstrip(' seconds'))
        return (payload)

    def on_log(self, client, userdata, level, buf):
        self.log.debug(buf)

    def _run(self):
        self.log.info("Start thread")
        self.init_db()
        try:
            self._mqtt_client.connect(self.mqtt_broker_host)
        except ValueError as e:
           self.log.error("MQTT " + str(e))
        except Exception as er:
           self.log.error("MQTT " + str(e))
        else:
            self._mqtt_client.loop_forever()
        self.log.info("Exit thread")

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(target=self._run)
            self._thread.daemon = True
            self._thread.start()

    def stop(self):
        if self._thread is not None:
            self._mqtt_client.disconnect()
            if threading.current_thread() != self._thread:
                self._thread.join()
                self._thread = None

    def close(self):
        self.stop()

def start_daemon(pidf, logf, is_debug=False):
    ### This launches the daemon in its context
    logger = logging.getLogger("mzproxy.err")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logf)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.info("start daemon")
    with daemon.DaemonContext(
        working_directory='/tmp',
        umask=0o002,
        pidfile=pidfile.TimeoutPIDLockFile(pidf),
        stderr =  fh.stream,
        stdout =  fh.stream,
        ) as context:
            mzp = cmzProxy(is_debug)
            try:
                while True:
                    time.sleep(.5)
            except:
                mzp.stop()
                time.sleep(.5)

def stop_daemon(pidf):
    if os.path.isfile(pidf):
        pid = int(open(pidf).read())
        os.kill(pid, signal.SIGTERM)

def start_foreground(is_debug=False):
    mzp = cmzProxy(is_debug)
    while True:
        try:
            time.sleep(.3)
        except KeyboardInterrupt:
            mzp.close()
            print "Bye"
            sys.exit()

# print json.dumps(list_items, sort_keys=True, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT to zabbix proxy")
    parser.add_argument('-f', '--foreground', action='store_true', help="Run mzproxy in foreground")
    parser.add_argument('-s', '--start', action='store_true', help="Start daemon")
    parser.add_argument('-t', '--stop', action='store_true', help="Stop daemon")
    parser.add_argument('-r', '--restart', action='store_true', help="Restart daemon")
    parser.add_argument('-d', '--debug', action='store_true', help="Start in debug mode")
    parser.add_argument('-p', '--pid-file', default='/tmp/mzproxy.pid')
    parser.add_argument('-l', '--log-err', default='/tmp/mzproxy.err')
    parser.add_argument('-c', '--config', default='/etc/mzproxy.conf')
    args = parser.parse_args()

    if args.start:
        start_daemon(pidf=args.pid_file, logf=args.log_err, is_debug=args.debug )
    elif args.stop:
        stop_daemon(pidf=args.pid_file)
    elif args.restart:
        stop_daemon(pidf=args.pid_file)
        time.sleep(1)
        start_daemon(pidf=args.pid_file, logf=args.log_err)
    elif args.foreground:
        start_foreground(is_debug=args.debug)
    else:
        parser.print_help()

