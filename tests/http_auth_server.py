import base64
import http.server
import socketserver

import click


@click.command()
@click.option("--address")
@click.option("--port")
@click.option("--username")
@click.option("--password")
def serve_forever(address, port, username, password):

    port = int(port)

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
    with socketserver.TCPServer((address, port), Handler) as httpd:
        httpd.serve_forever()


if __name__ == '__main__':
    serve_forever()
