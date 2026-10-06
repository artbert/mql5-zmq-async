import asyncio
import zmq
import zmq.asyncio
import json
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Address definitions (must be matched to MQL5 Service)
# According to the architecture: MT5 binds PULL/PUB, so Python does CONNECT.
# MT5 connects (Connect) to Python's PUSH, so Python does BIND.
PULL_FROM_MT5_ADDR = "tcp://127.0.0.1:5556"  # Python binds here as PULL
PUSH_TO_MT5_ADDR = "tcp://127.0.0.1:5555"  # Python connects as PUSH
SUB_MARKET_ADDR = "tcp://127.0.0.1:5557"  # Python connects as SUB


async def market_data_listener(ctx: zmq.asyncio.Context):
    """Asynchronous loop receiving market ticks (SUB)"""
    socket = ctx.socket(zmq.SUB)
    socket.connect(SUB_MARKET_ADDR)

    socket.setsockopt(zmq.RCVHWM, 1)
    socket.setsockopt(zmq.LINGER, 0)

    # We subscribe to all messages (empty string = everything)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")

    print("[SUB] Market data listening started...")

    try:
        while True:
            # Asynchronous reception (does not block the entire program).
            # In pyzmq.asyncio we do not need to explicitly specify ZMQ_DONTWAIT
            # because await returns control to the asyncio event loop.
            msg_bytes = await socket.recv()
            msg_str = msg_bytes.decode("utf-8")

            try:
                data = json.loads(msg_str)
                print(
                    f"[TICK] {data['symbol']} | Bid: {data['bid']} | Ask: {data['ask']}"
                )
            except json.JSONDecodeError:
                print(f"[SUB] Received an incorrect format: {msg_str}")

    except asyncio.CancelledError:
        socket.close()


async def order_response_listener(ctx: zmq.asyncio.Context):
    """Asynchronous loop receiving transaction confirmations (PULL)"""
    socket = ctx.socket(zmq.PULL)
    socket.bind(PULL_FROM_MT5_ADDR)

    socket.setsockopt(zmq.RCVHWM, 1)
    socket.setsockopt(zmq.LINGER, 0)

    print("[PULL] Listening for responses from MetaTrader started...")

    try:
        while True:
            msg_bytes = await socket.recv()
            msg_str = msg_bytes.decode("utf-8")
            print(f"[RESPONSE From MT5] {msg_str}")

    except asyncio.CancelledError:
        socket.close()


async def send_order_loop(ctx: zmq.asyncio.Context):
    """Sample loop sending test orders (PUSH) every 10 seconds"""
    socket = ctx.socket(zmq.PUSH)
    socket.connect(PUSH_TO_MT5_ADDR)

    socket.setsockopt(zmq.SNDHWM, 1)
    socket.setsockopt(zmq.LINGER, 0)

    print("[PUSH] Ready to send orders...")

    order_counter = 1
    try:
        while True:
            # Wait 10 seconds before the next order
            await asyncio.sleep(10)

            command = {
                "action": "OPEN",
                "type": "BUY",
                "symbol": "EURUSD",
                "volume": 0.01,
                "id": order_counter,
            }

            payload = json.dumps(command)
            print(f"[PUSH] I am sending order no. {order_counter}...")

            try:
                # We are pushing the order. Thanks to SNDHWM=1 and
                # the non-blocking mode in asyncio, if MT5 freezes, pyzmq will
                # throw a ZMQError(EAGAIN) exception instead of hanging.
                await socket.send_string(payload, flags=zmq.DONTWAIT)
                order_counter += 1
            except zmq.Again:
                print("[PUSH] Error: Buffer full! MetaTrader is not receiving orders.")

    except asyncio.CancelledError:
        socket.close()


async def main():
    # We are using a special asynchronous context with pyzmq
    ctx = zmq.asyncio.Context.instance()

    # We run all three tasks concurrently
    tasks = [
        asyncio.create_task(market_data_listener(ctx)),
        asyncio.create_task(order_response_listener(ctx)),
        asyncio.create_task(send_order_loop(ctx)),
    ]

    # Starting the main loop and handling program shutdown (Ctrl+C)
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\nClosing the client application...")
    finally:
        for task in tasks:
            task.cancel()
        ctx.term()
        print("ZeroMQ context cleared. Goodbye.")


if __name__ == "__main__":
    # Starting the asynchronous entry point
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
