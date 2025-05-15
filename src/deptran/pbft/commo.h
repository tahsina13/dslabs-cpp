#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../communicator.h"
#include "pbft_rpc.h"

namespace janus {

class TxData;

class PbftCommo : public Communicator {

 public:
  PbftCommo() = delete;
  PbftCommo(PollMgr*);

  void
  SendPreprepare(parid_t par_id,
                 siteid_t site_id, 
                 const PreprepareMessage& mesg,
                 const shared_ptr<Marshallable>& cmd,
                 const Request &req); 

  void
  SendPrepare(parid_t par_id,
              siteid_t site_id,
              const PrepareMessage& mesg);

  void
  SendCommit(parid_t par_id,
             siteid_t site_id,
             const CommitMessage& mesg);

  void 
  SendCheckpoint(parid_t par_id,
                 siteid_t site_id,
                 const CheckpointMessage& mesg);

  void
  SendViewChange(parid_t par_id,
                 siteid_t site_id,
                 const ViewChangeMessage& mesg);

  void
  SendNewView(parid_t par_id,
              siteid_t site_id,
              const NewViewMessage& mesg);

  /* Do not modify this class below here */

  friend class FpgaPbftProxy;
 public:
#ifdef PBFT_TEST_CORO
  std::recursive_mutex rpc_mtx_ = {};
  uint64_t rpc_count_ = 0;
#endif
};

} // namespace janus

