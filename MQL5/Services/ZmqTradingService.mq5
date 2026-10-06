//+------------------------------------------------------------------+
//|                                            ZmqTradingService.mq5 |
//|                                          Copyright 2026, artbert |
//|                                       https://github.com/artbert |
//+------------------------------------------------------------------+
#property service
#property copyright "Copyright 2026, artbert"
#property link "https://github.com/artbert"
#property version "1.00"

#include <ZmqLib.mqh>
// Required JAson library from https://github.com/vivazzi/JAson
#include <JAson.mqh>

// Ports and addresses definitions
#define PULL_ADDRESS "tcp://127.0.0.1:5555" // Commands for MT5
#define PUSH_ADDRESS "tcp://127.0.0.1:5556" // Statuses/Responses from MT5
#define PUB_ADDRESS "tcp://127.0.0.1:5557"  // Market data (Ticks)

// Global indicators on sockets
CZmqSocket *socketPull = NULL;
CZmqSocket *socketPush = NULL;
CZmqSocket *socketPub = NULL;

// Subscribed symbol (can be extended to an array)
string tradingSymbol = "EURUSD";
//+------------------------------------------------------------------+
//| Service program start function                                   |
//+------------------------------------------------------------------+
void OnStart()
{
    // 1. Socket initialization
    socketPull = new CZmqSocket(ZMQ_PULL, "ZMQ_DEFAULT_CTX", false);
    socketPush = new CZmqSocket(ZMQ_PUSH, "ZMQ_DEFAULT_CTX", false);
    socketPub = new CZmqSocket(ZMQ_PUB, "ZMQ_DEFAULT_CTX", false);

    if (!socketPull.IsValid() || !socketPush.IsValid() || !socketPub.IsValid())
    {
        Print("[ZMQ Service] Błąd: Nie udało się poprawnie utworzyć gniazd ZMQ.");
        Cleanup();
        return;
    }

    // 2. Network security parameters configuration
    ConfigureSocket(socketPull);
    ConfigureSocket(socketPush);
    ConfigureSocket(socketPub);

    // 3. Address binding (Bind / Connect).
    // Usually the server (MT5) does Bind for PULL and PUB, and Connect or Bind for PUSH depending on the Python architecture.
    // Here MT5 binds PULL and PUB, and connects (Connect) to Python's PULL (which here is PUSH).
    if (!socketPull.Bind(PULL_ADDRESS))
        Print("[ZMQ Service] Błąd Bind PULL");
    if (!socketPub.Bind(PUB_ADDRESS))
        Print("[ZMQ Service] Błąd Bind PUB");
    if (!socketPush.Connect(PUSH_ADDRESS))
        Print("[ZMQ Service] Błąd Connect PUSH");

    Print("[ZMQ Service] Uruchomiono poprawnie. Oczekiwanie na komunikację...");

    // Helper variables for handling market ticks
    MqlTick lastTick;
    ZeroMemory(lastTick);

    // 4. Main asynchronous Service loop (runs in the terminal background)
    while (!IsStopped())
    {
        // --- SECTION 1: Receiving trading commands (PULL) ---
        CZmqMsg incomingMsg;
        // We use the ZMQ_DONTWAIT flag passed directly to the Recv method
        if (socketPull.Recv(incomingMsg, ZMQ_DONTWAIT))
        {
            string command = incomingMsg.GetString();
            if (command != "")
            {
                Print("[ZMQ Service] Command received: ", command);
                ProcessIncomingCommand(command);
            }
        }

        // --- SECTION 2: Streaming market data (PUB) ---
        MqlTick currentTick;
        if (SymbolInfoTick(tradingSymbol, currentTick))
        {
            // We send data only when a new tick appears (time or price change)
            if (currentTick.time_msc != lastTick.time_msc)
            {
                lastTick = currentTick;

                // Format the tick into simple JSON or CSV
                string tickPayload = StringFormat("{\"symbol\":\"%s\",\"time\":%lld,\"bid\":%G,\"ask\":%G}",
                                                  tradingSymbol, currentTick.time_msc, currentTick.bid, currentTick.ask);

                CZmqMsg pubMsg;
                if (pubMsg.SetString(tickPayload))
                {
                    // ZMQ_DONTWAIT prevents thread blocking when the SUB client can't keep up
                    socketPub.Send(pubMsg, ZMQ_DONTWAIT);
                }
            }
        }

        // A short break for the processor (1 millisecond)
        Sleep(1);
    }

    // 5. Cleaning up resources after the service is done
    Cleanup();
}
//+------------------------------------------------------------------+
//| Auxiliary socket configuration                                   |
//+------------------------------------------------------------------+
void ConfigureSocket(CZmqSocket *socket)
{
    socket.SetSendHighWaterMark(1);
    socket.SetRecvHighWaterMark(1);
    socket.SetLinger(0);
}

//+------------------------------------------------------------------+
//| Processing a received text command                               |
//+------------------------------------------------------------------+
void ProcessIncomingCommand(string commandText)
{
    CJAVal json;

    // 1. Parsing text into a JSON object
    if (!json.Deserialize(commandText))
    {
        Print("[ZMQ Service] Błąd parsowania JSON: ", commandText);
        return;
    }

    // 2. Extracting values using keys (with explicit typecasting)
    string action = json["action"].ToStr();
    string type = json["type"].ToStr();
    string symbol = json["symbol"].ToStr();
    double volume = json["volume"].ToDbl();
    long orderId = json["id"].ToInt();

    PrintFormat("[ZMQ] Processing an order from Python ID: %d | Akcja: %s %s %s %.2f lota",
                orderId, action, type, symbol, volume);

    // 3. Sending an order to MT5 not implemented yet
    // Sending a quick acknowledgment of order receipt to Python
    SendResponseToPython(orderId, "SUBMITTED", orderId);
}

//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
void SendResponseToPython(long orderId, string status, ulong mt5OrderTicket)
{
    CJAVal response;

    // Building a JSON structure
    response["order_id"] = orderId;
    response["status"] = status;
    response["mt5_ticket"] = (long)mt5OrderTicket;
    response["timestamp"] = (long)TimeCurrent();

    // Serializing to a JSON text string
    string jsonText = response.Serialize();

    CZmqMsg responseMsg;
    if (responseMsg.SetString(jsonText))
    {
        if (!socketPush.Send(responseMsg, ZMQ_DONTWAIT))
        {
            Print("[ZMQ Service] Error sending response via PUSH - buffer full.");
        }
    }
}

//+------------------------------------------------------------------+
//| Safely freeing memory                                            |
//+------------------------------------------------------------------+
void Cleanup()
{
    Print("[ZMQ Service] Closing connections and cleaning up memory...");

    if (socketPull != NULL)
    {
        socketPull.Unbind(PULL_ADDRESS);
        delete socketPull;
    }
    if (socketPub != NULL)
    {
        socketPub.Unbind(PUB_ADDRESS);
        delete socketPub;
    }
    if (socketPush != NULL)
    {
        socketPush.Disconnect(PUSH_ADDRESS);
        delete socketPush;
    }
}
//+------------------------------------------------------------------+
