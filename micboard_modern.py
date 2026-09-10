
import time
import logging
import json
import os
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler
from providers import ShureProvider, SennheiserProvider

Devices = {}

class DataHandler(RequestHandler):
    def get(self):
        data = {ip: dev.get_json() for ip, dev in Devices.items()}
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(data))

class CommandHandler(RequestHandler):
    def post(self):
        params = json.loads(self.request.body)
        ip = params.get('ip')
        cmd = params.get('command')
        val = params.get('value')
        chan = params.get('channel', 1)
        
        if ip in Devices:
            success = Devices[ip].send_command(cmd, val, chan)
            self.write(json.dumps({'success': success}))
        else:
            self.set_status(404)

class ScanHandler(RequestHandler):
    def get(self):
        ip = self.get_argument('ip', None)
        if ip and ip in Devices:
            dev = Devices[ip]
            dev.scan_rf()
            self.write(json.dumps({'spectrum': dev.spectrum_data}))
        else: self.set_status(404)

class StreamHandler(RequestHandler):
    def get(self):
        ip = self.get_argument('ip', None)
        if not ip or ip not in Devices:
            self.set_status(404)
            return
        self.set_header('Content-Type', 'audio/mpeg')
        self.write(f"Audio stream proxy for {ip}")

class StaticHandler(RequestHandler):
    def get(self):
        path = self.get_argument('path')
        base_dir = os.path.abspath('.')
        full_path = os.path.abspath(os.path.join(base_dir, path))
        if not full_path.startswith(base_dir + os.sep):
            self.set_status(403); return
        if not os.path.isfile(full_path):
            self.set_status(404); return
        with open(full_path, 'rb') as f:
            self.write(f.read())

def poll_devices():
    for dev in Devices.values():
        dev.poll()
    IOLoop.current().call_later(1, poll_devices)

def load_config():
    config_path = 'config.json'
    if not os.path.exists(config_path): return []
    with open(config_path, 'r') as f:
        return json.load(f).get('devices', [])

def main():
    app = Application([
        (r'/data', DataHandler),
        (r'/command', CommandHandler),
        (r'/scan', ScanHandler),
        (r'/stream', StreamHandler),
        (r'/static/(.*)', StaticHandler),
    ])
    app.listen(9000, address='0.0.0.0')
    
    device_list = load_config()
    for dev_cfg in device_list:
        ip = dev_cfg['ip']
        brand = dev_cfg.get('brand', 'shure')
        dtype = dev_cfg.get('type', 'axtd')
        Devices[ip] = ShureProvider(ip, dtype) if brand == 'shure' else SennheiserProvider(ip, dtype)
        Devices[ip].connect()

    poll_devices()
    print("Micboard Pro Command Center running on 0.0.0.0:9000...")
    IOLoop.current().start()

if __name__ == '__main__':
    main()
