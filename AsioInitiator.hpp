#ifndef ASIO_FIX_ASIO_INITIATOR_HPP
#define ASIO_FIX_ASIO_INITIATOR_HPP

#include "AsioConnection.hpp"

#include <quickfix/HostDetailsProvider.h>
#include <quickfix/Initiator.h>
#include <quickfix/Session.h>
#include <quickfix/SessionID.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/basic_resolver.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace FIX {

class AsioInitiator : public Initiator, private boost::noncopyable {
public:
  AsioInitiator(
      boost::asio::io_context &ioContext,
      Application &application,
      MessageStoreFactory &messageStoreFactory,
      const SessionSettings &settings) EXCEPT(ConfigError)
      : Initiator(application, messageStoreFactory, settings),
        m_ioContext(ioContext) {}

  AsioInitiator(
      boost::asio::io_context &ioContext,
      Application &application,
      MessageStoreFactory &messageStoreFactory,
      const SessionSettings &settings,
      LogFactory &logFactory) EXCEPT(ConfigError)
      : Initiator(application, messageStoreFactory, settings, logFactory),
        m_ioContext(ioContext) {}

  ~AsioInitiator() override {}

private:
  // Strategy

  /// Implemented to configure acceptor
  virtual void onConfigure(const SessionSettings &) override EXCEPT(ConfigError) {};
  /// Implemented to initialize initiator
  virtual void onInitialize(const SessionSettings &) override EXCEPT(RuntimeError) { connect(); };

  /// Implemented to start connecting to targets.
  virtual void onStart() override {
    // io_context should be polled globally - do nothing here
  }

  /// Implemented to connect and poll for events.
  virtual bool onPoll() override {
    // io_context should be polled globally - do nothing here
    return false;
  }

  /// Implemented to stop a running initiator.
  virtual void onStop() override { m_ioContext.stop(); }

  /// Implemented to connect a session to its target.
  virtual void doConnect(const SessionID &s, const Dictionary &d) override try {
    Session *session = Session::lookupSession(s);
    if (!session->isSessionTime(UtcTimeStamp::now())) {
      return;
    }

    HostDetails host = m_hostDetailsProvider.getHost(s, d);
    if (d.has(RECONNECT_INTERVAL)) // ReconnectInterval in [SESSION]
    {
      m_reconnectInterval = d.getInt(RECONNECT_INTERVAL);
    }

    getLog()->onEvent(
        "Connecting to " + host.address + " on port " + IntConvertor::convert((unsigned short)host.port) + " (Source "
        + host.sourceAddress + ":" + IntConvertor::convert((unsigned short)host.sourcePort)
        + ") ReconnectInterval=" + IntConvertor::convert((int)m_reconnectInterval));

    boost::system::error_code ec;
    boost::asio::ip::tcp::socket socket(m_ioContext);
    boost::asio::ip::tcp::resolver resolver(m_ioContext);

    boost::asio::ip::tcp::resolver::results_type endpoints
        = resolver.resolve(host.address, std::to_string(host.port), ec);
    if (ec.failed()) {
      std::stringstream ss;
      ss << "unable to resolve " << host.address << ":" << host.port;
      throw std::runtime_error(ss.str());
    }

    boost::asio::connect(socket, endpoints, ec);
    if (ec.failed()) {
      std::stringstream ss;
      ss << "unable to connect " << host.address << ":" << host.port;
      throw std::runtime_error(ss.str());
    }

    auto connection = std::make_shared<AsioConnection<>>(std::move(socket), getLog(), s, session);
    connection->start();
  } catch (std::exception &e) {
    getLog()->onEvent(e.what());
  }

protected:
  boost::asio::io_context &m_ioContext;
  HostDetailsProvider m_hostDetailsProvider;

  int m_reconnectInterval = 30;
};

} // namespace FIX

#endif // ASIO_FIX_ASIO_SOCKET_INITIATOR_HPP
