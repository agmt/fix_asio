#ifndef ASIO_FIX_ASIO_WS_ESTABLISHING_CONNECTION_HPP
#define ASIO_FIX_ASIO_WS_ESTABLISHING_CONNECTION_HPP

#include "AsioConnection.hpp"

#include <boost/beast.hpp>

namespace FIX {

template <typename underlying_socket_t = boost::asio::ip::tcp::socket>
class AsioWSEstablishingConnection
    : public std::enable_shared_from_this<AsioWSEstablishingConnection<underlying_socket_t>> {

protected:
  using socket_t = boost::beast::websocket::stream<underlying_socket_t>;

  socket_t m_socket;
  Log *m_pLog;
  Acceptor *m_acceptor;
  boost::beast::multi_buffer m_incoming_buffer;
  boost::beast::http::request<boost::beast::http::empty_body> m_http_request;
  boost::beast::http::response<boost::beast::http::string_body> m_http_response;

public:
  AsioWSEstablishingConnection(underlying_socket_t socket, Log *log, Acceptor *acceptor)
      : m_socket(std::move(socket)),
        m_pLog(log),
        m_acceptor(acceptor) {}

  void start() { this->http_accept(); }

  void stop() {
    if (m_pLog != nullptr) {
      m_pLog->onEvent("stop establishing WS");
    }
  }

  void close() {
    boost::system::error_code ec;
    boost::beast::get_lowest_layer(m_socket).close(ec);
  }

  void http_accept() {
    boost::beast::http::async_read(
        m_socket.next_layer(),
        m_incoming_buffer,
        m_http_request,
        [Self = this->shared_from_this()](boost::beast::error_code Error, size_t bytes_transferred) {
          if (!boost::beast::get_lowest_layer(Self->m_socket).is_open()) {
            // Stopping.
            return;
          }

          if (Error) {
            return Self->stop();
          }

          if (boost::beast::websocket::is_upgrade(Self->m_http_request)) {
            Self->websocket_accept();
          } else {
            Self->http_error_response();
          }
        });
  }

  void websocket_accept() {
    boost::beast::websocket::permessage_deflate pmd;
    pmd.client_enable = true;
    pmd.server_enable = true;
    pmd.msg_size_threshold = 8192; // Compress only big messages
    m_socket.set_option(pmd);
    // m_socket.compress(false); // Boost >= 1.81 feature to control per-message compression
    m_socket.auto_fragment(false);

    m_socket.async_accept(m_http_request, [Self = this->shared_from_this()](boost::system::error_code Error) {
      if (!boost::beast::get_lowest_layer(Self->m_socket).is_open()) {
        // Stopping.
      } else if (Error) {
        Self->stop();
      } else {
        auto sharedConnection
            = std::make_shared<AsioConnection<socket_t>>(std::move(Self->m_socket), Self->m_pLog, Self->m_acceptor);

        sharedConnection->start();
      }
    });
  }

  void http_error_response() {
    m_http_response.result(boost::beast::http::status::bad_request);
    m_http_response.version(m_http_response.version());
    m_http_response.keep_alive(false);
    boost::beast::http::async_write(
        m_socket.next_layer(),
        m_http_response,
        [Self = this->shared_from_this()](boost::system::error_code Error, std::size_t) {
          if (!boost::beast::get_lowest_layer(Self->m_socket).is_open()) {
            // Stopping.
            return;
          }

          if (Error) {
            return Self->close();
          }

          Self->close();
        });
  }
};

} // namespace FIX

#endif // ASIO_FIX_ASIO_WS_ESTABLISHING_CONNECTION_HPP
