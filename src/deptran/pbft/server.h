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
  uint64_t target_ckpt_majority_; 

  uint64_t current_view_; 
  siteid_t current_primary_; 

  slotid_t low_watermark_; 
  slotid_t high_watermark_; 
  slotid_t last_executed_;

  std::map<slotid_t, Request> requests_; 
  std::map<std::pair<uint64_t, cliid_t>, std::string> replies_;
  LogStore logstore_; 

  bool in_view_change_; 
  
  const EVP_MD *md_; 
  EVP_MD_CTX *mdctx_; 

  EVP_PKEY_CTX *privkey_ctx_; 
  std::map<siteid_t, std::shared_ptr<EVP_PKEY_CTX>> pubkey_ctx_; 

  /* Your functions here */
  std::string GetDigest(const std::shared_ptr<Marshallable> &cmd);

  std::string SignHash(const std::string &hash); 
  bool VerifyHash(const std::string &hash, const std::string &signature, siteid_t site_id);

  PreprepareMessage CreatePreprepare(uint64_t view, slotid_t seqno, const std::string &digest); 
  PrepareMessage CreatePrepare(uint64_t view, slotid_t seqno, const std::string &digest);
  CommitMessage CreateCommit(uint64_t view, slotid_t seqno, const std::string &digest);
  CheckpointMessage CreateCheckpoint(slotid_t ckpt_seqno, const std::string &ckpt_digest);
  ViewChangeMessage CreateViewChange(uint64_t new_view); 
  NewViewMessage CreateNewView(uint64_t new_view, const std::map<siteid_t, ViewChangeMessage> &view_changes);

  std::string GetPreprepareHash(const PreprepareMessage &mesg);
  std::string GetPrepareHash(const PrepareMessage &mesg);
  std::string GetCommitHash(const CommitMessage &mesg);
  std::string GetCheckpointHash(const CheckpointMessage &mesg);
  std::string GetViewChangeHash(const ViewChangeMessage &mesg);
  std::string GetNewViewHash(const NewViewMessage &mesg);

  void SignPreprepare(PreprepareMessage &mesg); 
  void SignPrepare(PrepareMessage &mesg);
  void SignCommit(CommitMessage &mesg);
  void SignCheckpoint(CheckpointMessage &mesg);
  void SignViewChange(ViewChangeMessage &mesg);
  void SignNewView(NewViewMessage &mesg);

  bool VerifyPreprepare(const PreprepareMessage &mesg);
  bool VerifyPrepare(const PrepareMessage &mesg);
  bool VerifyCommit(const CommitMessage &mesg);
  bool VerifyCheckpoint(const CheckpointMessage &mesg);
  bool VerifyViewChange(const ViewChangeMessage &mesg); 
  bool VerifyNewView(const NewViewMessage &mesg); 
  
  void HandlePreprepare(const PreprepareMessage &mesg); 
  void HandlePrepare(const PrepareMessage &mesg); 
  void HandleCommit(const CommitMessage &mesg); 
  void HandleCheckpoint(const CheckpointMessage &mesg); 
  void HandleViewChange(const ViewChangeMessage &mesg); 
  void HandleNewView(const NewViewMessage &mesg); 
  
  void OnPreprepare(const PreprepareMessage &mesg, 
                    const std::shared_ptr<Marshallable> &cmd,
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
