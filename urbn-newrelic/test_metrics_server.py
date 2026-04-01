#!/usr/bin/env python3
"""
A proper Prometheus metrics server for testing
"""
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from datetime import datetime

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/metrics':
            # Generate current metrics
            counter_value = int(time.time()) % 100  # Changes over time
            gauge_value = 42.5 + (time.time() % 10)  # Slowly varying gauge
            timestamp = datetime.now().strftime('%H:%M:%S')
            
            metrics = f"""# HELP test_counter A test counter
# TYPE test_counter counter
test_counter{{env="local"}} {counter_value}

# HELP test_gauge A test gauge  
# TYPE test_gauge gauge
test_gauge{{env="local"}} {gauge_value:.1f}

# HELP up Whether the target is up
# TYPE up gauge
up 1
"""
            
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Content-Length', str(len(metrics)))
            self.end_headers()
            self.wfile.write(metrics.encode('utf-8'))
            print(f"[{timestamp}] 📊 Served metrics: counter={counter_value}, gauge={gauge_value:.1f} to {self.client_address[0]}")
        
        elif self.path == '/startup':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
            
        elif self.path == '/liveness':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Suppress default logging, we'll do our own
        pass

def run_server():
    server = HTTPServer(('0.0.0.0', 8080), MetricsHandler)
    print("🚀 Metrics server starting on http://0.0.0.0:8080")
    print("📊 Metrics available at http://0.0.0.0:8080/metrics")
    print("💓 Health checks at /startup and /liveness")
    print("🕐 Scrapes every 10s, exports to New Relic in batches")
    print("Press Ctrl+C to stop")
    print()
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Shutting down metrics server")
        server.shutdown()

if __name__ == '__main__':
    run_server()
