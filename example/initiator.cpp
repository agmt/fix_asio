#include "../AsioInitiator.hpp"
#include "quickfix/Application.h"
#include "quickfix/Log.h"
#include "quickfix/MessageStore.h"
#include "quickfix/SessionSettings.h"

class MyApplication : public FIX::Application {
public:
  void onCreate(const FIX::SessionID &sessionID) override {
    std::cout << "Create - " << sessionID.toString() << std::endl;
  }

  // Called when the session successfully logs on
  void onLogon(const FIX::SessionID &sessionID) override {
    std::cout << "Logon - " << sessionID.toString() << std::endl;
  }

  // Called when the session logs out
  void onLogout(const FIX::SessionID &sessionID) override {
    std::cout << "Logout - " << sessionID.toString() << std::endl;
  }

  // Other required methods (empty implementations for simplicity)
  void fromAdmin(const FIX::Message &, const FIX::SessionID &) override {}
  void toAdmin(FIX::Message &, const FIX::SessionID &) override {}
  void fromApp(const FIX::Message &, const FIX::SessionID &) override {}
  void toApp(FIX::Message &, const FIX::SessionID &) override {}
};

int main(int argc, char **argv) {
  try {
    // Create application instance
    MyApplication application;

    // Load session settings from configuration file
    FIX::SessionSettings settings("initiator.cfg");

    // Use file store for message persistence
    FIX::MemoryStoreFactory storeFactory;

    // Use file logging
    FIX::ScreenLogFactory logFactory(true, true, true);

    // Create and start the initiator (client)
    boost::asio::io_context ioContext;
    FIX::AsioInitiator initiator(ioContext, application, storeFactory, settings, logFactory);
    initiator.start();

    ioContext.run();
    // Keep the program running to allow the initiator to maintain the session
    std::cout << "Press enter to quit..." << std::endl;
    std::cin.get();

    // Stop the initiator
    initiator.stop();
  } catch (FIX::ConfigError &e) {
    std::cerr << "Configuration error: " << e.what() << std::endl;
    return 1;
  } catch (FIX::RuntimeError &e) {
    std::cerr << "Runtime error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
