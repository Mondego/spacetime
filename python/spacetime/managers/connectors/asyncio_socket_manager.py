import asyncio
import socket
import traceback
from threading import Thread

import spacetime.utils.utils as utils
import spacetime.utils.enums as enums
from spacetime.utils.socket_utils import send_ack, send_data, recv_ack, recv_data
from spacetime.utils.utils import instrument_func


async def create_connection(loop, details):
    reader, writer = await asyncio.open_connection(
        details[0], details[1], loop=loop)

    return reader, writer


class AIOSocketConnector(object):
    @property
    def has_parent_connection(self):
        return self.parent is not None

    def __init__(self, appname, parent, details, types, instrument_q):
        self.appname = appname
        self.parent = parent
        self.details = details
        self.types = types
        self.parent_version = None
        self.parent_version = "ROOT"
        # Logger for SocketManager
        self.logger = utils.get_logger("%s_SocketConnector" % self.appname)
        self.loop = asyncio.new_event_loop()
        self.instrument_record = instrument_q
        self.reader, self.writer = (
            self.loop.run_until_complete(
                create_connection(self.loop, self.parent))
            if self.parent else
            (None, None))

    def get_new_version(self, new_versions):
        return (new_versions[1])

    async def _push(self, diff_data, version):
        try:
            package = {
                enums.TransferFields.AppName: self.appname,
                enums.TransferFields.RequestType: enums.RequestType.Push,
                enums.TransferFields.Versions: version,
                enums.TransferFields.Data: diff_data
            }
            await send_data(self.writer, package)
            succ = await recv_ack(self.reader)
            self.parent_version = self.get_new_version(version)
            return succ
        except Exception as e:
            print ("PUSH", e)
            print(traceback.format_exc())
            raise

    async def _pull(self):
        try:
            data = {
                enums.TransferFields.AppName: self.appname,
                enums.TransferFields.RequestType: enums.RequestType.Pull,
                enums.TransferFields.Versions: self.parent_version
            }

            await send_data(self.writer, data)
            self.logger.debug("Data sent (pull req)")

            self.logger.debug("Data received (pull req).")
            # Versions
            resp = await recv_data(self.reader)

            # Doesnt matter if connection was lost in the next function.
            # Just the ack failed.
            # Maintain wont happen on server, but its not the end of the world.
            # It can happen next cycle.
            await send_ack(self.writer)
            self.logger.debug("Ack sent (pull req)")
            new_versions = resp[enums.TransferFields.Versions]
            # Actual payload
            package = resp[enums.TransferFields.Data]
            # Send bool status back.
            self.parent_version = self.get_new_version(new_versions)
            return package, new_versions
        except Exception as e:
            print ("PULL", e)
            print(traceback.format_exc())
            raise

    @instrument_func("send_push")
    def push_req(self, diff_data, version):
        if self.parent:
            return self.loop.run_until_complete(
                self._push(diff_data, version))
        return True

    @instrument_func("send_pull")
    def pull_req(self):
        if self.parent:
            return self.loop.run_until_complete(self._pull())
        return dict(), None


class AIOSocketServer(Thread):
    def __init__(self, appname, server_port,
                 pull_call_back, push_call_back, confirm_pull_req,
                 instrument_q):
        # Logger for SocketManager
        self.logger = utils.get_logger("%s_SocketManager" % appname)

        # Call back function to process incoming pull requests.
        self.pull_call_back = pull_call_back

        # Call back function to process incoming push requests.
        self.push_call_back = push_call_back

        # Call back function to confirm incoming pull requests to dataframe.
        self.confirm_pull_req = confirm_pull_req

        # The socket that can be used by children.
        self.sync_socket = self.setup_socket(server_port)

        # The socket details.
        addr, port = self.sync_socket.getsockname()

        self.port = (addr if addr != "0.0.0.0" else "127.0.0.1", port)

        super().__init__()
        self.daemon = True
        self.server = None
        self.loop = None
        self.instrument_record = instrument_q

    def setup_socket(self, server_port):
        sync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # sync_socket.settimeout(2)
        sync_socket.bind(("", server_port))
        sync_socket.listen()
        self.logger.debug("Socket bound")
        return sync_socket

    def run(self):
        self.loop = asyncio.new_event_loop()
        coro = asyncio.start_server(
            self.handle_client, loop=self.loop, sock=self.sync_socket)
        self.server = self.loop.run_until_complete(coro)

        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.server.close()
            self.loop.run_until_complete(self.server.wait_closed())
            self.loop.close()

    @instrument_func("handle_client")
    async def handle_client(self, reader, writer):
        while True:  # Till the client breaks or the connection has to be ended.
            try:
                data = await recv_data(reader)
                if data[enums.TransferFields.RequestType] == enums.RequestType.Push:
                    self.process_push(data)
                    await send_ack(writer)
                elif data[
                        enums.TransferFields.RequestType] == enums.RequestType.Pull:
                    resp = self.process_pull(data)
                    await send_data(writer, resp)
                    succ = await recv_ack(reader)
                    if not succ:
                        print ("Ack failed", succ)
                        break
                    self.process_data_sent_confirmed(data, resp)
            except Exception as e:
                print ("SERVER", e)
                # Ideally, handle and close connection.
                # The client probably disconnected
                # But we can raise it is fine.
                raise

        writer.close()

    def process_push(self, data):
        req_app = data[enums.TransferFields.AppName]
        versions = data[enums.TransferFields.Versions]
        package = data[enums.TransferFields.Data]
        self.push_call_back(req_app, versions, package)

    def process_pull(self, data):
        req_app = data[enums.TransferFields.AppName]
        versions = data[enums.TransferFields.Versions]
        dict_to_send, new_versions = self.pull_call_back(req_app, versions)
        data_to_send = {
            enums.TransferFields.AppName: self.port,
            enums.TransferFields.Data: dict_to_send,
            enums.TransferFields.Versions: new_versions}
        return data_to_send

    def process_data_sent_confirmed(self, req, resp):
        self.confirm_pull_req(
            req[enums.TransferFields.AppName],
            resp[enums.TransferFields.Versions])
