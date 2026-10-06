//+------------------------------------------------------------------+
//|                                                       ZmqLib.mqh |
//|                                   Direct libzmq binding for MQL5 |
//|                                         Copyright 2026, artbert  |
//|                                       https://github.com/artbert |
//+------------------------------------------------------------------+
#property copyright "Copyright 2026, artbert"
#property link "https://github.com/artbert"
#property strict

#ifdef __MQL5__
#define __X64__
#endif

#ifdef __X64__
#define intptr_t long
#define size_t ulong
#else
#define intptr_t int
#define size_t uint
#endif

#ifdef _DEBUG // This macro exists exclusively during compilation in debug mode
#define DebugPrintIf(cond, text) \
    if (cond)                    \
    Print("DEBUG: ", text)
#else
#define DebugPrintIf(cond, text) // In production mode, this command is ignored
#endif

// --- libzmq options ---
#define ZMQ_PAIR 0
#define ZMQ_PUB 1
#define ZMQ_SUB 2
#define ZMQ_REQ 3
#define ZMQ_REP 4
#define ZMQ_DEALER 5
#define ZMQ_ROUTER 6
#define ZMQ_PULL 7
#define ZMQ_PUSH 8

#define ZMQ_LINGER 17
#define ZMQ_SNDHWM 23
#define ZMQ_RCVHWM 24
#define ZMQ_BLOCKY 70

#define ZMQ_DONTWAIT 1
#define ZMQ_SNDMORE 2

// --- Aligned zmq_msg_t structure for 64-bit platform ---
struct zmq_msg_t
{
    uchar _[64]; // In libzmq v4.x, zmq_msg_t has a size of 64 bytes
};

// --- Imports from system libraries and DLLs ---
// --- memcpy is from the standard C/C++ library (more Linux/Wine friendly)
#import "msvcrt.dll"
size_t memcpy(uchar &hpvDest[], intptr_t hpvSource, size_t cbCopy);
size_t memcpy(intptr_t hpvDest, const uchar &hpvSource[], size_t cbCopy);
#import

#import "libzmq.dll"
// Context
intptr_t zmq_ctx_new(void);
int zmq_ctx_term(intptr_t context);
int zmq_ctx_set(intptr_t context, int option, int optval);

// Socket
intptr_t zmq_socket(intptr_t context, int type);
int zmq_close(intptr_t socket);
int zmq_setsockopt(intptr_t s, int option, const int &optval, size_t optvallen);
int zmq_bind(intptr_t s, const char &addr[]);
int zmq_unbind(intptr_t s, const char &addr[]);
int zmq_connect(intptr_t s, const char &addr[]);
int zmq_disconnect(intptr_t s, const char &addr[]);

// Messages
int zmq_msg_init(zmq_msg_t &msg);
int zmq_msg_init_size(zmq_msg_t &msg, size_t size);
int zmq_msg_close(zmq_msg_t &msg);
intptr_t zmq_msg_data(zmq_msg_t &msg);
size_t zmq_msg_size(zmq_msg_t &msg);
int zmq_msg_send(zmq_msg_t &msg, intptr_t s, int flags);
int zmq_msg_recv(zmq_msg_t &msg, intptr_t s, int flags);
#import

//+------------------------------------------------------------------+
//| ZeroMQ message wrapper                                           |
//| The beginning of the CZmqMsg class object address points         |
//| to the beginning of the zmq_msg_t structure.                     |
//| When libzmq receives a pointer to this object (this),            |
//| it manipulates only the first 64 bytes of this object.           |
//+------------------------------------------------------------------+
struct CZmqMsg : zmq_msg_t
{
public:
    CZmqMsg() { zmq_msg_init(this); }

    CZmqMsg(size_t size)
    {
        bool cond = zmq_msg_init_size(this, size) != 0;
        DebugPrintIf(cond, "Failed to init msg of size: " + IntegerToString(size));
    }

    ~CZmqMsg()
    {
        bool cond = zmq_msg_close(this) != 0;
        DebugPrintIf(cond, "Failed to close msg");
    }

    // Retrieving message content into a byte array
    void GetData(uchar &out_data[])
    {
        size_t size = zmq_msg_size(this);
        ArrayResize(out_data, (int)size);
        if (size > 0)
        {
            intptr_t ptr = zmq_msg_data(this);
            memcpy(out_data, ptr, size);
        }
    }

    // Retrieving message content as a String
    string GetString()
    {
        uchar data[];
        GetData(data);
        return CharArrayToString(data, 0, WHOLE_ARRAY, CP_UTF8);
    }

    // Setting content from a string
    bool SetString(string text)
    {
        uchar data[];
        int len = StringToCharArray(text, data, 0, WHOLE_ARRAY, CP_UTF8);
        // StringToCharArray adds a null-terminator at the end – usually we omit it in ZMQ
        if (len > 0 && data[len - 1] == 0)
            len--;

        if (zmq_msg_init_size(this, (size_t)len) != 0)
        {
            return false;
        }

        if (len > 0)
        {
            intptr_t ptr = zmq_msg_data(this);
            memcpy(ptr, data, (size_t)len);
        }
        return true;
    }
};

//+------------------------------------------------------------------+
//| Shared context manager in the MetaTrader process                 |
//+------------------------------------------------------------------+
class CZmqContext
{
private:
    static intptr_t s_context;
    static string s_ctx_name;

    // Auxiliary mutex lock based on GlobalVariableTemp
    static bool Lock(string mutex_name, int timeout_ms = 1000)
    {
        uint start = GetTickCount();
        while (GetTickCount() - start < (uint)timeout_ms)
        {
            if (GlobalVariableTemp(mutex_name))
                if (GlobalVariableSet(mutex_name, 1.0) != 0)
                    return true;
            Sleep(5);
        }
        return false;
    }

    static void Unlock(string mutex_name)
    {
        GlobalVariableTemp(mutex_name);
        GlobalVariableDel(mutex_name);
    }

public:
    // Setting the ZMQ_BLOCKY option for the context
    static bool SetBlocky(intptr_t ctx_ptr, bool blocky)
    {
        if (ctx_ptr == 0)
            return false;
        return zmq_ctx_set(ctx_ptr, ZMQ_BLOCKY, blocky ? 1 : 0) == 0;
    }
    static intptr_t Acquire(string context_name = "ZMQ_DEFAULT_CTX", bool blocky = false)
    {
        string mutex = "ZMQ_MUTEX_" + context_name;
        string ref_cnt_var = "ZMQ_REF_" + context_name;
        string ptr_var = "ZMQ_PTR_" + context_name;

        if (!Lock(mutex))
            return 0;

        intptr_t ctx_ptr = 0;
        GlobalVariableTemp(ref_cnt_var);
        GlobalVariableTemp(ptr_var);

        double refs = 0;
        GlobalVariableGet(ref_cnt_var, refs);

        if (refs <= 0)
        {
            // Creating a new context
            ctx_ptr = zmq_ctx_new();
            if (ctx_ptr != 0)
            {
                // ZMQ_BLOCKY configuration (by default we set it to false for safe context termination in MT5)
                SetBlocky(ctx_ptr, blocky);
                GlobalVariableSet(ref_cnt_var, 1.0);
                GlobalVariableSet(ptr_var, (double)ctx_ptr);
            }
        }
        else
        {
            // Fetching the existing context and increasing its number of references
            double ptr_val = 0;
            GlobalVariableGet(ptr_var, ptr_val);
            ctx_ptr = (intptr_t)ptr_val;
            GlobalVariableSet(ref_cnt_var, refs + 1.0);
        }

        Unlock(mutex);
        return ctx_ptr;
    }

    static void Release(string context_name = "ZMQ_DEFAULT_CTX")
    {
        string mutex = "ZMQ_MUTEX_" + context_name;
        string ref_cnt_var = "ZMQ_REF_" + context_name;
        string ptr_var = "ZMQ_PTR_" + context_name;

        if (!Lock(mutex))
            return;

        GlobalVariableTemp(ref_cnt_var);
        GlobalVariableTemp(ptr_var);

        double refs = 0;
        GlobalVariableGet(ref_cnt_var, refs);

        long current_refs = (long)MathRound(refs);
        if (current_refs > 1)
        {
            // Other threads still exist, so we only decrease the counter
            GlobalVariableSet(ref_cnt_var, (double)(current_refs - 1));
        }
        else if (current_refs <= 1 && current_refs > 0)
        {
            // This was the last thread (or due to an error, the value dropped below 1). We destroy the context
            double ptr_val = 0;
            GlobalVariableGet(ptr_var, ptr_val);
            intptr_t ctx_ptr = (intptr_t)ptr_val;

            if (ctx_ptr != 0)
            {
                bool cond = zmq_ctx_term(ctx_ptr) != 0;
                DebugPrintIf(cond, "Failed to destroy context");
            }

            // Cleaning up process variables
            GlobalVariableDel(ref_cnt_var);
            GlobalVariableDel(ptr_var);
        }

        Unlock(mutex);
    }
};

// Initialization of static members
intptr_t CZmqContext::s_context = 0;
string CZmqContext::s_ctx_name = "";

//+------------------------------------------------------------------+
//| Base class for Sockets                                           |
//+------------------------------------------------------------------+
class CZmqSocket
{
protected:
    intptr_t m_socket;
    string m_ctx_name;

public:
    CZmqSocket(int type, string context_name = "ZMQ_DEFAULT_CTX", bool blocky = false) : m_socket(0), m_ctx_name(context_name)
    {
        intptr_t ctx = CZmqContext::Acquire(m_ctx_name, blocky);
        if (ctx != 0)
        {
            m_socket = zmq_socket(ctx, type);
        }
    }

    ~CZmqSocket()
    {
        if (m_socket != 0)
        {
            bool cond = zmq_close(m_socket) != 0;
            DebugPrintIf(cond, StringFormat("Failed to close socket %d" + m_socket));
            m_socket = 0;
        }
        CZmqContext::Release(m_ctx_name);
    }

    bool IsValid() const { return m_socket != 0; }

    bool SetSendHighWaterMark(int count)
    {
        return zmq_setsockopt(m_socket, ZMQ_SNDHWM, count, sizeof(int)) == 0;
    }

    bool SetRecvHighWaterMark(int count)
    {
        return zmq_setsockopt(m_socket, ZMQ_RCVHWM, count, sizeof(int)) == 0;
    }

    bool SetLinger(int ms)
    {
        return zmq_setsockopt(m_socket, ZMQ_LINGER, ms, sizeof(int)) == 0;
    }

    bool Bind(string address)
    {
        char addr[];
        StringToCharArray(address, addr, 0, WHOLE_ARRAY, CP_UTF8);
        return zmq_bind(m_socket, addr) == 0;
    }

    bool Unbind(string address)
    {
        char addr[];
        StringToCharArray(address, addr, 0, WHOLE_ARRAY, CP_UTF8);
        return zmq_unbind(m_socket, addr) == 0;
    }

    bool Connect(string address)
    {
        char addr[];
        StringToCharArray(address, addr, 0, WHOLE_ARRAY, CP_UTF8);
        return zmq_connect(m_socket, addr) == 0;
    }

    bool Disconnect(string address)
    {
        char addr[];
        StringToCharArray(address, addr, 0, WHOLE_ARRAY, CP_UTF8);
        return zmq_disconnect(m_socket, addr) == 0;
    }

    bool Send(CZmqMsg &msg, int flags = 0)
    {
        return zmq_msg_send(msg, m_socket, flags) >= 0;
    }

    bool Recv(CZmqMsg &msg, int flags = 0)
    {
        return zmq_msg_recv(msg, m_socket, flags) >= 0;
    }
};
//+------------------------------------------------------------------+
