#ifndef ASIO_FIX_ASIO_CONNECTION_HPP
#define ASIO_FIX_ASIO_CONNECTION_HPP

#include <quickfix/Acceptor.h>
#include <quickfix/Exceptions.h>
#include <quickfix/Parser.h>
#include <quickfix/Session.h>

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/bind/bind.hpp>
#include <boost/noncopyable.hpp>

#include <cmath>
#include <memory>
#include <optional>

#define INCOMING_BUFFER_SIZE 65536
#define OUTGOING_BUFFER_SIZE 65536

namespace FIX {

// This helper is required because
//   - boost::asio::ip::tcp::socket doesn't have `.async_write()`
//   - boost::beast::websocket::stream doesn't supported by `boost::asio::async_write()`
// Solution: add a helper to make all underlying sockets to support `.async_write()`
template <typename Protocol, typename Executor>
class stream_socket_with_write : public boost::asio::basic_stream_socket<Protocol, Executor> {
public:
  template <typename... ArgsT> auto async_write(ArgsT &&...args) {
    return boost::asio::async_write(*this, std::forward<ArgsT>(args)...);
  }
};

using tcp_socket_t = stream_socket_with_write<boost::asio::ip::tcp, boost::asio::any_io_executor>;

template <typename socket_t = boost::asio::ip::tcp::socket>
class AsioConnection : public std::enable_shared_from_this<AsioConnection<socket_t>>,
                       public Responder,
                       private boost::noncopyable {
protected:
  using real_socket_type
      = std::conditional<std::is_same_v<socket_t, boost::asio::ip::tcp::socket>, tcp_socket_t, socket_t>::type;

  real_socket_type m_socket;

  SessionID m_session_id;
  Session *m_pSession;
  SessionState *m_pSessionState;

  Log *m_pLog;
  Parser m_parser;
  using clock_t = std::chrono::system_clock;
  boost::asio::system_timer HeartbeatTimer_;
  std::array<char, INCOMING_BUFFER_SIZE> m_incoming_buffer;
  std::array<char, OUTGOING_BUFFER_SIZE> m_outgoing_buffer;
  std::deque<char> m_queued_outgoing_buffer;
  bool m_sending;

public:
  // Constructed by Initiator: known Session
  AsioConnection(socket_t socket, Log *log, SessionID const &sessionID, Session *session)
      : m_socket(std::move(socket)),
        m_session_id(sessionID),
        m_pSession(session),
        m_pLog(log),
        HeartbeatTimer_(m_socket.get_executor()),
        m_sending(false) {
    assert(m_socket.is_open());
    assert(session != 0);
  }

  // Constructed by Acceptor: Session will be known after 1st message = Logon
  AsioConnection(socket_t socket, Log *log, Acceptor *acceptor)
      : m_socket(std::move(socket)),
        m_pSession(nullptr),
        m_pLog(log),
        HeartbeatTimer_(m_socket.get_executor()),
        m_sending(false) {
    assert(m_socket.is_open());
    assert(acceptor != nullptr);
  }

  ~AsioConnection() {
    stop();

    // This destructor can be called asynchronously, so `m_pLog` can be destroyed
    // ToDo: own FIX::Log
    /*if( m_pLog )
    {
        m_pLog->onEvent( "~AsioSocketConnection" );
    }*/
  }

  void start() {
    if (m_pSession) {
      m_pSession->setResponder(this);
      m_pSession->next(UtcTimeStamp::now()); // should send logon
      registerSession();
    }
    StartReadAsync();
  }

  bool send(const std::string &msg) override {
    size_t sz = msg.size();
    const char *buf = msg.data();
    if (m_pLog) {
      m_pLog->onOutgoing(msg);
    }
    if (!m_sending && sz <= OUTGOING_BUFFER_SIZE) {
      StartSendAsync(buf, sz);
    } else {
      m_queued_outgoing_buffer.insert(m_queued_outgoing_buffer.end(), msg.begin(), msg.end());
      if (!m_sending) {
        m_sending = true;
        StartSendAsync();
      }
    }
    return true;
  }

  // Called internally by QuickFIX reasons (connection is alive, QuickFIX state is Disconnect)
  void disconnect() override {
    if (m_pLog) {
      SessionID sessionID;
      if (m_pSession) {
        sessionID = m_pSession->getSessionID();
      }
      m_pLog->onEvent(std::string("Disconnect ") + sessionID.toStringFrozen());
    }
    if (m_socket.is_open()) {
      boost::system::error_code ec;
      boost::beast::get_lowest_layer(m_socket).close(ec);
      if (m_pLog) {
        if (ec) {
          m_pLog->onEvent(std::string("Disconnect ec: ") + ec.what());
        }
      }
    }

    // FIX::SocketConnection calls unregisterSession() in destructor
    // But this class is async and we want to support Session removal
    if (m_pSession) {
      unregisterSession();
    }

    // After calling disconnect(), `FIX::Session*` can be deleted
    m_pSession = nullptr;
  }

protected:
  // Called by network reasons (connection is dead, QuickFIX state is Active (to be disconnected))
  void stop() {
    if (m_pSession) {
      FIX::Session *Session = m_pSession;
      Session->disconnect(); // Calls m_Responder->disconnect() (i.e. this ::disconnect() ) and nulls m_Responder
    }
  }

  void StartSendAsync() {
    assert(m_sending);
    assert(m_socket.is_open());
    if (m_queued_outgoing_buffer.size() == 0) {
      m_sending = false;
    } else {
      size_t sz = (m_queued_outgoing_buffer.size() > OUTGOING_BUFFER_SIZE) ? OUTGOING_BUFFER_SIZE
                                                                           : m_queued_outgoing_buffer.size();
      std::copy(m_queued_outgoing_buffer.begin(), m_queued_outgoing_buffer.begin() + sz, m_outgoing_buffer.begin());
      m_queued_outgoing_buffer.erase(m_queued_outgoing_buffer.begin(), m_queued_outgoing_buffer.begin() + sz);
      m_socket.async_write(
          boost::asio::const_buffer(m_outgoing_buffer.data(), sz),
          [Self = this->shared_from_this()](boost::system::error_code ec, size_t len) {
            Self->AsyncSentSocket(ec, len);
          });
    }
  }

  void StartSendAsync(char const *buf, size_t sz) {
    assert(m_socket.is_open());
    assert(!m_sending);
    m_sending = true;
    std::copy(buf, buf + sz, m_outgoing_buffer.begin());
    m_socket.async_write(
        boost::asio::const_buffer(m_outgoing_buffer.data(), sz),
        [Self = this->shared_from_this()](boost::system::error_code ec, size_t len) {
          Self->AsyncSentSocket(ec, len);
        });
  }

  void StartReadAsync() {
    assert(m_socket.is_open());
    m_socket.async_read_some(
        boost::asio::buffer(m_incoming_buffer),
        [Self = this->shared_from_this()](boost::system::error_code ec, size_t len) {
          Self->AsyncReadSocket(ec, len);
        });
  }

  void AsyncReadSocket(boost::system::error_code const &error_code, size_t len) try {
    if ((error_code.failed()) || (len == 0)) {
      if (m_pLog) {
        m_pLog->onEvent("socket disconnected or error received!");
      }
      stop();
      return;
    }

    if (!m_socket.is_open()) {
      return;
    }

    std::string msg;
    const char *buffer = m_incoming_buffer.begin();
    m_parser.addToStream(buffer, len);

    if (!m_pSession) {
      if (!m_parser.readFixMessage(msg)) {
        return StartReadAsync();
      }

      std::optional<SessionID> sessionIDOpt = getSessionID(msg, true);
      if (!sessionIDOpt.has_value()) {
        if (m_pLog) {
          m_pLog->onEvent("Invalid incoming message: " + msg);
          m_pLog->onIncoming(msg);
        }
        boost::system::error_code ec;
        boost::beast::get_lowest_layer(m_socket).cancel(ec);
        return;
      }
      SessionID sessionID = sessionIDOpt.value();
      m_pSession = Session::lookupSession(sessionIDOpt.value());
      if (m_pSession == nullptr) {
        if (m_pLog) {
          m_pLog->onEvent("Session not found for incoming message: " + msg);
          m_pLog->onIncoming(msg);
        }
        DoLogout(sessionIDOpt.value(), "API Key not found");
        return;
      }

      sessionID = m_pSession->getSessionID();
      if (Session::isSessionRegistered(sessionID)) {
        m_pSession = nullptr;
        if (m_pLog) {
          m_pLog->onEvent("Session is already registered: " + msg);
          m_pLog->onIncoming(msg);
        }
        DoLogout(sessionIDOpt.value(), "API Key is already connected");
        return;
      }

      if (m_pSession) {
        m_pSession->setResponder(this);
        m_pSession->next(msg, UtcTimeStamp::now());
      }

      if (!m_pSession) {
        boost::system::error_code ec;
        boost::beast::get_lowest_layer(m_socket).cancel(ec);
        if (m_pLog) {
          m_pLog->onEvent("Session not found for incoming message, disconnecting: " + msg);
          if (ec) {
            m_pLog->onEvent(" ec: " + ec.what());
          }
          m_pLog->onIncoming(msg);
        }
        return;
      }

      registerSession();
    }

    while (m_parser.readFixMessage(msg)) {
      // QuickFIX may call Responder::disconnect() by itself
      if (m_pSession == nullptr) {
        if (m_pLog) {
          m_pLog->onEvent("Session closed during parsing: " + msg);
        }
        return;
      }

      m_pSession->next(msg, UtcTimeStamp::now());
    }

    // QuickFIX may call Responder::disconnect() by itself
    if (m_socket.is_open()) {
      StartReadAsync();
    }
  } catch (FIX::InvalidMessage &e) {
    // Already reported by QuickFIX
    /*
    if( m_pLog )
    {
        m_pLog->onEvent( e.what() );
    }
    */
    stop();
  } catch (...) {
    if (m_pLog) {
      m_pLog->onEvent("exception");
    }
    stop();
  }

  void AsyncSentSocket(boost::system::error_code const &error_code, size_t len) try {
    if ((error_code.failed()) || (len == 0)) {
      if (m_pLog) {
        m_pLog->onEvent("socket disconnected or error received!");
      }
      // stop(); // Disconnect is to be called in AsyncReadSocket()
      return;
    }

    if (m_socket.is_open()) {
      StartSendAsync();
    }
  } catch (...) {
    if (m_pLog) {
      m_pLog->onEvent("exception");
    }
    stop();
  }

  void StartAsyncTimeout() {
    if (!m_pSessionState) {
      return;
    }

    int testRequest = m_pSessionState->testRequest();

    double nextTestRequestInSecondsDouble
        = ((1.2 * ((double)testRequest + 1))
           * (double)m_pSessionState->heartBtInt()); // from FIX::SessionState::needTestRequest()
    time_t nextTestRequestInSeconds
        = std::ceil(nextTestRequestInSecondsDouble); // QuickFIX compares `operator-()` vs `double`
                                                     // (both are casted to double) => round up

    // operator-(DateTime,DateTime) truncates nanos before subtraction => truncate nanos before adding
    auto nextHousekeeping = m_pSessionState->lastReceivedTime().getTimeT() + nextTestRequestInSeconds;
    if (!testRequest) {
      auto needHeartbeatAt = m_pSessionState->lastSentTime().getTimeT()
                             + (time_t)m_pSessionState->heartBtInt(); // from FIX::SessionState::needHeartbeat()
      nextHousekeeping = std::min(nextHousekeeping, needHeartbeatAt);
    }

    HeartbeatTimer_.expires_at(clock_t::from_time_t(nextHousekeeping));
    HeartbeatTimer_.async_wait([WSelf = this->weak_from_this()](boost::system::error_code const &error_code) {
      auto Self = WSelf.lock();
      if (Self) {
        Self->AsyncTimeout(error_code);
      }
    });
  }

  void AsyncTimeout(boost::system::error_code const &error_code) {
    if (!m_pSession) {
      return;
    }
    m_pSession->next(UtcTimeStamp::now());
    if (!m_pSession) {
      return;
    }
    StartAsyncTimeout();
  }

  void registerSession() {
    Session::registerSession(m_pSession->getSessionID());
    m_pSessionState
        = dynamic_cast<SessionState *>(m_pSession->getLog()); // rely on the current specific behaviour of QuickFIX
    StartAsyncTimeout();
  }

  void unregisterSession() {
    HeartbeatTimer_.cancel();
    Session::unregisterSession(m_pSession->getSessionID());
    m_pSessionState = nullptr;
  }

  static std::optional<SessionID> getSessionID(const std::string &string, bool reverse) {
    Message message;
    if (!message.setStringHeader(string)) {
      return std::nullopt;
    }
    try {
      const Header &header = message.getHeader();
      const BeginString &beginString = FIELD_GET_REF(header, BeginString);
      const SenderCompID &senderCompID = FIELD_GET_REF(header, SenderCompID);
      const TargetCompID &targetCompID = FIELD_GET_REF(header, TargetCompID);

      if (reverse) {
        return SessionID(beginString, SenderCompID(targetCompID), TargetCompID(senderCompID));
      }

      return SessionID(beginString, senderCompID, targetCompID);
    } catch (FieldNotFound &) {
      return std::nullopt;
    }
  }

  void DoLogout(SessionID sessionID, const std::string &text) {
    Message logout;

    logout.getHeader().setField(MsgType(FIX::MsgType_Logout));

    logout.getHeader().setField(sessionID.getBeginString());
    logout.getHeader().setField(sessionID.getSenderCompID());
    logout.getHeader().setField(sessionID.getTargetCompID());
    logout.getHeader().setField(MsgSeqNum(1));
    logout.getHeader().setField(SendingTime(UtcTimeStamp::now(), 9));

    if (text.length()) {
      logout.setField(Text(text));
    }

    std::string msg = logout.toString();
    send(msg);

    boost::system::error_code ec;
    boost::beast::get_lowest_layer(m_socket).shutdown(boost::asio::socket_base::shutdown_receive, ec);
    boost::beast::get_lowest_layer(m_socket).cancel(ec);
  }
};

} // namespace FIX

#endif // ASIO_FIX_ASIO_SOCKET_CONNECTION_HPP
