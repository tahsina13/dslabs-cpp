#pragma once

#include <openssl/evp.h>

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include "pbft_rpc.h"
#include "commo.h"
#include "logstore.h"

namespace janus {

#define CHECKPOINT_INTERVAL 100
#define MAX_REQUESTS_IN_TRANSIT (2 * CHECKPOINT_INTERVAL)

enum RequestState {
  REQ_INIT,
  REQ_PREPARED,
  REQ_COMMITTED,
  REQ_EXECUTED,
}; 

struct Request {
  std::shared_ptr<Marshallable> cmd;
  uint64_t timestamp; 
  cliid_t client_id; 
  RequestState state; 
  // TODO: implement timers 
}; 

class PbftServer : public TxLogServer {
 public:
  /* Your data here */
  uint64_t num_proxies_; 
  uint64_t target_majority_; 

  uint64_t current_view_; 
  locid_t current_primary_; 

  slotid_t low_watermark_; 
  slotid_t high_watermark_; 
  slotid_t last_executed_;

  std::map<slotid_t, Request> requests_; 
  std::map<std::pair<uint64_t, cliid_t>, std::string> replies_;
  LogStore logstore_; 

  bool in_view_change_; 
  
  const EVP_MD *md_; 
  EVP_MD_CTX *mdctx_; 

  /* Your functions here */
  PreprepareMessage CreatePreprepare(uint64_t view, slotid_t seqno); 
  PrepareMessage CreatePrepare(uint64_t view, slotid_t seqno);
  CommitMessage CreateCommit(uint64_t view, slotid_t seqno);
  CheckpointMessage CreateCheckpoint(slotid_t ckpt_seqno);
  ViewChangeMessage CreateViewChange(uint64_t new_view); 
  NewViewMessage CreateNewView(uint64_t new_view, const std::map<locid_t, ViewChangeMessage> &view_changes);

  void OnPreprepare(const PreprepareMessage &mesg, 
                    std::shared_ptr<Marshallable> cmd,
                    uint64_t timestamp,
                    cliid_t client_id,
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

  bool Start(shared_ptr<Marshallable> &cmd, uint64_t timestamp, cliid_t client_id, uint64_t *index, uint64_t *view); 
  void GetState(bool *is_primary, uint64_t *view); 
  bool GetReply(uint64_t timestamp, cliid_t client_id, string *reply);

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
