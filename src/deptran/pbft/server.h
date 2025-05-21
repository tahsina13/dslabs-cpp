#pragma once

#include <openssl/evp.h>

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include "pbft_rpc.h"
#include "commo.h"
#include "logstore.h"
#include "authenticator.h"

namespace janus {

#define CHKPT_INTERVAL 100
#define MAX_REQUESTS_IN_TRANSIT (2 * CHKPT_INTERVAL)

enum RequestState {
  REQ_INIT,
  REQ_PREPARED,
  REQ_COMMITTED,
  REQ_EXECUTED,
}; 

struct ServerRequest {
  std::shared_ptr<Marshallable> cmd; 
  Request req; 
  RequestState state; 
  // TODO: implement timers. use alarm.hpp/alarm.cpp?

  ServerRequest(std::shared_ptr<Marshallable> cmd,
                const Request &req)
    : cmd(cmd), req(req), state(REQ_INIT) {}
}; 

class PbftServer : public TxLogServer {
 public:
  /* Your data here */
  uint64_t num_proxies_; 
  uint64_t target_majority_; 
  uint64_t target_chkpt_majority_; 

  uint64_t current_view_; 
  svrid_t current_primary_; 

  slotid_t low_watermark_;  // also latest stable checkpoint sequence number
  slotid_t high_watermark_; 
  slotid_t last_executed_;

  bool in_view_change_; 

  std::map<slotid_t, ServerRequest> requests_; 
  std::map<std::pair<uint64_t, cliid_t>, Reply> replies_;
  LogStore logstore_; 
  
  const EVP_MD *md_; 
  Authenticator privkey_auth_; 
  std::map<siteid_t, Authenticator> pubkey_auth_; 

  /* Your functions here */
  PreprepareMessage CreatePreprepare(uint64_t view, slotid_t seqno, const std::string &digest); 
  PrepareMessage CreatePrepare(uint64_t view, slotid_t seqno, const std::string &digest);
  CommitMessage CreateCommit(uint64_t view, slotid_t seqno, const std::string &digest);
  CheckpointMessage CreateCheckpoint(slotid_t chkpt_seqno, const std::string &chkpt_digest);
  ViewChangeMessage CreateViewChange(uint64_t new_view); 
  NewViewMessage CreateNewView(uint64_t new_view, const std::map<svrid_t, ViewChangeMessage> &view_changes);
  
  void HandlePreprepare(const PreprepareMessage &mesg); 
  void HandlePrepare(const PrepareMessage &mesg); 
  void HandleCommit(const CommitMessage &mesg); 
  void HandleCheckpoint(const CheckpointMessage &mesg); 
  void HandleViewChange(const ViewChangeMessage &mesg); 
  void HandleNewView(const NewViewMessage &mesg); 
  
  void OnPreprepare(const PreprepareMessage &mesg, 
                    const std::shared_ptr<Marshallable> &cmd,
                    const Request &req,
                    const function<void()> &cb);
  void OnPrepare(const PrepareMessage &mesg, const function<void()> &cb);
  void OnCommit(const CommitMessage &mesg, const function<void()> &cb);
  void OnCheckpoint(const CheckpointMessage &mesg, const function<void()> &cb);
  void OnViewChange(const ViewChangeMessage &mesg, const function<void()> &cb);
  void OnNewView(const NewViewMessage &mesg, const function<void()> &cb);

  /* do not modify this class below here */

 public:
  PbftServer(Frame *frame) ;
  ~PbftServer() ;

  bool Start(const shared_ptr<Marshallable> &cmd, 
             const Request &req,
             uint64_t *index, uint64_t *view); 
  void GetState(bool *is_primary, uint64_t *view, uint64_t *chkpt_seqno); 
  bool GetReply(uint64_t timestamp, cliid_t client_id, Reply *rep);

 private:
  bool disconnected_ = false;
	void Setup();

 public:
  void Disconnect(const bool disconnect = true);
  void Reconnect() {
    Disconnect(false);
  }
  bool IsDisconnected();

  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    verify(0);
  };
  PbftCommo* commo() {
    return (PbftCommo*)commo_;
  }
};
} // namespace janus
