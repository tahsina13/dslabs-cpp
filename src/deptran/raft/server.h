#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include "raft_rpc.h"
#include "commo.h"

namespace janus {

#define HEARTBEAT_INTERVAL 100000 // 100ms

#define MIN_ELECTION_TIMEOUT 2500000 // 2500ms 
#define MAX_ELECTION_TIMEOUT 5000000 // 5000ms 

struct RaftData {
  std::shared_ptr<Marshallable> cmd;
  uint64_t term;
}; 

class RaftServer : public TxLogServer {
 private:
  enum State {
    LEADER,
    FOLLOWER,
    CANDIDATE,
  };
  
 public:
  /* Your data here */
  uint64_t num_proxies; 
  uint64_t target_majority;
  State state; 

  // Persistent state on all servers
  uint64_t current_term;
  locid_t voted_for;
  std::vector<RaftData> log;

  // Volitile state on all servers
  uint64_t commit_index;
  uint64_t last_applied;

  // Volitile state on leaders
  std::vector<uint64_t> next_index;
  std::vector<uint64_t> match_index;

  std::shared_ptr<IntEvent> heartbeat_event; 
  std::vector<std::shared_ptr<BoxEvent<bool_t>>> append_events; 

  std::mt19937 rng; 
  std::uniform_int_distribution<uint64_t> dist; 
  uint64_t election_timeout; 

  /* Your functions here */
  void UpdateTerm(uint64_t term); 
  void SendAppendEntries(); 
  void StartElection(); 
  void WaitForHeartbeat(uint64_t timeout); 

  void OnRequestVote(const uint64_t &term,
                     const locid_t &candidate_id,
                     const uint64_t &last_log_index,
                     const uint64_t &last_log_term,
                     uint64_t *ret_term,
                     bool_t *vote_granted,
                     const function<void()> &cb);

  void OnAppendEntries(const uint64_t &term,
                       const locid_t &leader_id,
                       const uint64_t &prev_log_index,
                       const uint64_t &prev_log_term,
                       const std::vector<RaftData> &entries,
                       const uint64_t &leader_commit,
                       bool_t *followerAppendOK,
                       const function<void()> &cb);

  void OnEmptyAppendEntries(const uint64_t &term,
                            const locid_t &leader_id,
                            const uint64_t &leader_commit,
                            const function<void()> &cb);
  
  /* do not modify this class below here */

 public:
  RaftServer(Frame *frame) ;
  ~RaftServer() ;

  bool Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term);
  void GetState(bool *is_leader, uint64_t *term);

 private:
  bool disconnected_ = false;
	void Setup();

 public:
  void SyncRpcExample();
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
  RaftCommo* commo() {
    return (RaftCommo*)commo_;
  }
};
} // namespace janus
