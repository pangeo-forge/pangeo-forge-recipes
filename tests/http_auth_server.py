import base64
import http.server
import socketserver
import sys

port_str, ADDRESS = sys.argv[1:3]
PORT = int(port_str)
if len(sys.argv) > 3:
    username, password = sys.argv[3:5]
else:
    username, password = "", ""


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if username:
            auth = self.headers.get("Authorization")
            if (
                auth is None
                or not auth.startswith("Basic")
                or auth[6:]
                != str(base64.b64encode((username + ":" + password).encode("utf-8")), "utf-8")
            ):
                self.send_response(401)
                self.send_header("WWW-Authenticate", "Basic")
                self.end_headers()
                return
        return http.server.SimpleHTTPRequestHandler.do_GET(self)


socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer((ADDRESS, PORT), Handler) as httpd:
    httpd.serve_forever()
