#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "raft_rpc.h"
#include "server.h"
#include "macros.h"

class SimpleCommand;
namespace janus {

class TxLogServer;
class RaftServer;
class RaftServiceImpl : public RaftService {
 public:
  RaftServer* svr_;
  RaftServiceImpl(TxLogServer* sched);

  RpcHandler(RequestVote, 6,
             const uint64_t&, term,
             const locid_t&, candidate_id,
             const uint64_t&, last_log_index,
             const uint64_t&, last_log_term,
             uint64_t*, ret_term,
             bool_t*, vote_granted) {
    *ret_term = 0; 
    *vote_granted = false;
  }

  RpcHandler(AppendEntries, 7,
             const uint64_t&, term,
             const locid_t&, leader_id,
             const uint64_t&, prev_log_index,
             const uint64_t&, prev_log_term,
             const vector<MarshallDeputyLogEntry>&, entries,
             const uint64_t&, leader_commit,
             bool_t*, followerAppendOK) {
    *followerAppendOK = false;
  }
  
  RpcHandler(EmptyAppendEntries, 3,
             const uint64_t&, term,
             const locid_t&, leader_id,
             const uint64_t&, leader_commit) { }

  RpcHandler(HelloRpc, 2, const string&, req, string*, res) {
    *res = "error"; 
  };

};

} // namespace janus
