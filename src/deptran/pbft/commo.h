#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../communicator.h"
#include "raft_rpc.h"

namespace janus {

class TxData;

class PbftCommo : public Communicator {

 public:
  PbftCommo() = delete;
  PbftCommo(PollMgr*);

  void
  SendPreprepare(parid_t par_id,
                 siteid_t site_id, // -1 for broadcast
                 const PreprepareRequest& req,
                 const Message& cmd); 

  void
  SendPrepare(parid_t par_id,
              siteid_t site_id, // -1 for broadcast
              const PrepareRequest& req);

  void
  SendCommit(parid_t par_id,
             siteid_t site_id, // -1 for broadcast
             const CommitRequest& req);

  void
  SendPrepared(parid_t par_id,
               siteid_t site_id, // -1 for broadcast
               const PreparedRequest& req);

  void
  SendCommitted(parid_t par_id,
                siteid_t site_id, // -1 for broadcast
                const CommittedRequest& req);

  void 
  SendCheckpoint(parid_t par_id,
                 siteid_t site_id, // -1 for broadcast
                 const CheckpointRequest& req);

  void
  SendViewChange(parid_t par_id,
                 siteid_t site_id, // -1 for broadcast
                 const ViewChangeRequest& req);

  void
  SendNewView(parid_t par_id,
              siteid_t site_id, // -1 for broadcast
              const NewViewRequest& req);

  /* Do not modify this class below here */

  friend class FpgaPbftProxy;
 public:
#ifdef PBFT_TEST_CORO
  std::recursive_mutex rpc_mtx_ = {};
  uint64_t rpc_count_ = 0;
#endif
};

} // namespace janus

