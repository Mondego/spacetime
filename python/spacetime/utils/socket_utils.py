from struct import pack, unpack
import traceback
import cbor

def receive_data(con, length):
    stack = list()
    while length:
        data = con.recv(length)
        stack.append(data)
        length = length - len(data)

    return b"".join(stack)

def send_all(con, data):
    while data:
        sent = con.send(data)
        if len(data) == sent:
            break
        data = data[sent:]

async def send_data(writer, data):
    try:
        raw_data = cbor.dumps(data)
        writer.write(pack("!L", len(raw_data)))
        writer.write(raw_data)
        await writer.drain()
    except Exception as e:
        print (e)
        raise

async def send_ack(writer):
    try:
        writer.write(pack("!?", True))
        succ = await writer.drain()
    except Exception as e:
        print (e)
        raise

async def recv_ack(reader):
    try:
        ack = await reader.read(n=1)
        return unpack("!?", ack)[0]
    except Exception as e:
        print (e)
        raise

async def recv_data(reader):
    try:
        con_succ = await reader.read(n=4)
        content_length = unpack("!L", con_succ)[0]
        data = await read_all(reader, content_length)
        return cbor.loads(data)
    except Exception as e:
        print (e)
        print (con_succ)
        raise

async def read_all(reader, content_length):
    try:
        remaining = content_length
        data = list()
        while remaining:
            data_part = await reader.read(n=remaining)
            data.append(data_part)
            remaining -= len(data_part)
        return b"".join(data)
    except Exception as e:
        print (e)
        raise
