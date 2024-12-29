#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../communicator.h"
#include "raft_rpc.h"

namespace janus {

class TxData;

class RaftCommo : public Communicator {

 public:
  RaftCommo() = delete;
  RaftCommo(PollMgr*);

  std::shared_ptr<SharedIntEvent>
  SendRequestVote(parid_t par_id,
                  siteid_t site_id,
                  uint64_t term,
                  locid_t candidate_id,
                  uint64_t last_loast_index,
                  uint64_t last_log_term,
                  uint64_t *ret_term);

  std::shared_ptr<BoxEvent<bool_t>>
  SendAppendEntries(parid_t par_id,
                    siteid_t site_id,
                    uint64_t term,
                    locid_t leader_id,
                    uint64_t prev_log_index,
                    uint64_t prev_log_term,
                    const std::vector<MarshallDeputyLogEntry>& entries,
                    uint64_t leader_commit);

  void
  SendEmptyAppendEntries(parid_t par_id,
                         siteid_t site_id,
                         uint64_t term,
                         locid_t leader_id,
                         uint64_t leader_commit);

  std::shared_ptr<IntEvent> 
  SendString(parid_t par_id, siteid_t site_id, const string& msg, string* res);

  /* Do not modify this class below here */

  friend class FpgaRaftProxy;
 public:
#ifdef RAFT_TEST_CORO
  std::recursive_mutex rpc_mtx_ = {};
  uint64_t rpc_count_ = 0;
#endif
};

} // namespace janus

