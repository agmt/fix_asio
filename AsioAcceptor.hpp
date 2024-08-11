#ifndef ASIO_FIX_ASIO_ACCEPTOR_HPP
#define ASIO_FIX_ASIO_ACCEPTOR_HPP

#include "AsioConnection.hpp"

#include <quickfix/Acceptor.h>

#include <boost/asio/ip/tcp.hpp>

namespace FIX {

class AsioSocketConnection;

class AsioTCPAcceptorServer : public std::enable_shared_from_this<AsioTCPAcceptorServer> {
public:
  AsioTCPAcceptorServer(boost::asio::io_context &ioContext, Acceptor *socketAcceptor, uint16_t port)
      : m_ioContext(ioContext),
        m_socketAcceptor(socketAcceptor),
        m_localEndpoint(boost::asio::ip::address_v4{}, port),
        m_acceptor(ioContext.get_executor(), m_localEndpoint) {}

  AsioTCPAcceptorServer(const AsioTCPAcceptorServer &) = delete;
  AsioTCPAcceptorServer(AsioTCPAcceptorServer &&) = delete;

  void doAccept() {
    m_acceptor.async_accept(
        [Self = this->shared_from_this()](boost::system::error_code ec, boost::asio::ip::tcp::socket socket) {
          std::stringstream ss;
          if (ec.failed()) {
            ss << "Accept error: " << ec.to_string();
            Self->m_socketAcceptor->getLog()->onEvent(ss.str());
            std::this_thread::sleep_for(std::chrono::microseconds(10));
            Self->doAccept();
            return;
          }

          boost::system::error_code lec;
          auto RemoteEndpoint = socket.remote_endpoint(lec);
          auto LocalEndpoint = socket.local_endpoint(lec);
          ss << "Accepted connection from " << RemoteEndpoint.address() << " on port " << LocalEndpoint.port();
          Self->m_socketAcceptor->getLog()->onEvent(ss.str());
          auto sharedConnection = std::make_shared<AsioConnection>(
              std::move(socket),
              Self->m_socketAcceptor->getLog(),
              Self->m_socketAcceptor);

          sharedConnection->start();
          Self->doAccept();
        });
  }

protected:
  boost::asio::io_context &m_ioContext;
  Acceptor *m_socketAcceptor;
  boost::asio::ip::tcp::endpoint m_localEndpoint;
  boost::asio::ip::tcp::acceptor m_acceptor;
};

/// Socket implementation of Acceptor.
class AsioAcceptor : public Acceptor {
public:
  AsioAcceptor(
      boost::asio::io_context &ioContext,
      Application &application,
      MessageStoreFactory &messageStoreFactory,
      const SessionSettings &sessionSettings) EXCEPT(ConfigError)
      : Acceptor(application, messageStoreFactory, sessionSettings),
        m_ioContext(ioContext) {}

  AsioAcceptor(
      boost::asio::io_context &ioContext,
      Application &application,
      MessageStoreFactory &messageStoreFactory,
      const SessionSettings &sessionSettings,
      LogFactory &logFactory) EXCEPT(ConfigError)
      : Acceptor(application, messageStoreFactory, sessionSettings, logFactory),
        m_ioContext(ioContext) {}

  virtual ~AsioAcceptor() {}

private:
  void onConfigure(const SessionSettings &s) override EXCEPT(ConfigError) {
    std::set<SessionID> sessions = s.getSessions();
    std::set<SessionID>::iterator i;
    for (i = sessions.begin(); i != sessions.end(); ++i) {
      const Dictionary &settings = s.get(*i);
      settings.getInt(SOCKET_ACCEPT_PORT);
      if (settings.has(SOCKET_REUSE_ADDRESS)) {
        settings.getBool(SOCKET_REUSE_ADDRESS);
      }
      if (settings.has(SOCKET_NODELAY)) {
        settings.getBool(SOCKET_NODELAY);
      }
    }
  }

  void onInitialize(const SessionSettings &s) override EXCEPT(RuntimeError) {
    uint16_t port = 0;

    try {
      std::set<SessionID> sessions = s.getSessions();
      std::set<SessionID>::iterator i = sessions.begin();
      for (; i != sessions.end(); ++i) {
        const Dictionary &settings = s.get(*i);
        port = (uint16_t)settings.getInt(SOCKET_ACCEPT_PORT);

        std::string protocol = "TCP";
        if (settings.has("SOCKET_PROTOCOL")) {
          protocol = settings.getString("SOCKET_PROTOCOL");
        }

        /*
        const bool reuseAddress = settings.has( SOCKET_REUSE_ADDRESS ) ?
        settings.getBool( SOCKET_REUSE_ADDRESS ) : true;

        const bool noDelay = settings.has( SOCKET_NODELAY ) ?
        settings.getBool( SOCKET_NODELAY ) : false;

        const int sendBufSize = settings.has( SOCKET_SEND_BUFFER_SIZE ) ?
        settings.getInt( SOCKET_SEND_BUFFER_SIZE ) : 0;

        const int rcvBufSize = settings.has( SOCKET_RECEIVE_BUFFER_SIZE ) ?
        settings.getInt( SOCKET_RECEIVE_BUFFER_SIZE ) : 0;
        */

        auto itServer = m_portToServer.find(port);
        if (itServer == m_portToServer.end()) {
          bool ok;
          auto server = std::make_shared<AsioTCPAcceptorServer>(m_ioContext, this, port);
          server->doAccept();
          std::tie(itServer, ok) = m_portToServer.try_emplace(port, server);
        }
      }
    } catch (SocketException &e) {
      throw RuntimeError(
          "Unable to create, bind, or listen to port " + IntConvertor::convert((unsigned short)port) + " (" + e.what()
          + ")");
    }
  }

  void onStart() override {
    // io_context should be polled globally - do nothing here
  }

  bool onPoll() override {
    // io_context should be polled globally - do nothing here
    return true;
  }

  void onStop() override { m_ioContext.stop(); }

protected:
  boost::asio::io_context &m_ioContext;
  std::map<uint16_t, std::shared_ptr<AsioTCPAcceptorServer>> m_portToServer;
};

} // namespace FIX

#endif // ASIO_FIX_ASIO_SOCKET_ACCEPTOR_HPP
