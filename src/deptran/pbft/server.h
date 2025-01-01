#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include "pbft_rpc.h"
#include "commo.h"

namespace janus {

class PbftServer : public TxLogServer {
 public:
  /* Your data here */
  uint64_t num_proxies_; 
  uint64_t target_majority_; 

  uint64_t current_view_; 

  // TODO: figure out how to handle watermarks
  slotid_t low_watermark_; 
  slotid_t high_watermark_; 

  // TODO: need to store message state
  std::map<slotid_t, PreprepareRequest> preprepares_; 
  std::map<slotid_t, std::map<locid_t, PrepareRequest>> prepares_; 
  std::map<slotid_t, std::map<locid_t, CommitRequest>> commits_; 
  std::map<slotid_t, std::map<locid_t, CheckpointRequest>> checkpoints_; 
  std::map<std::pair<uint64_t, cliid_t>, std::string> results_;
  
  /* Your functions here */
  void OnPreprepare(const PreprepareRequest &req, 
                    const Message &mesg,
                    const function<void()> &cb);
  void OnPrepare(const PrepareRequest &req, const function<void()> &cb);
  void OnCommit(const CommitRequest &req, const function<void()> &cb);
  
  void OnPrepared(const PreparedRequest &req, const function<void()> &cb);
  void OnCommitted(const CommittedRequest &req, const function<void()> &cb);

  void OnCheckpoint(const CheckpointRequest &req, const function<void()> &cb);

  void OnNewView(const NewViewRequest &req, const function<void()> &cb);
  void OnViewChange(const ViewChangeRequest &req, const function<void()> &cb);
  
  /* do not modify this class below here */

 public:
  PbftServer(Frame *frame) ;
  ~PbftServer() ;

  bool Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term);
  void GetState(bool *is_primary, uint64_t *view); 

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
