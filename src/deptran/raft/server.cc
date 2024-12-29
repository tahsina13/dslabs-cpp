

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"


namespace janus {

RaftServer::RaftServer(Frame * frame) : state(State::CANDIDATE),
                                        current_term(0),
                                        voted_for(-1),
                                        logs(1, { nullptr, 0 }),
                                        commit_index(0),
                                        last_applied(0),
                                        rng(std::random_device()()),
                                        dist(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT) {
  frame_ = frame ;
  /* Your code here for server initialization. Note that this function is 
     called in a different OS thread. Be careful about thread safety if 
     you want to initialize variables here. */

}

RaftServer::~RaftServer() {
  /* Your code here for server teardown */

}

void RaftServer::Setup() {
  /* Your code here for server setup. Due to the asynchronous nature of the 
     framework, this function could be called after a RPC handler is triggered. 
     Your code should be aware of that. This function is always called in the 
     same OS thread as the RPC handlers. */
  num_proxies = commo()->rpc_par_proxies_[partition_id_].size();
  target_majority = (num_proxies / 2) + 1; 
  Coroutine::CreateRun([this] {
    while (true) {
      switch (state) {
        case State::LEADER:
          SendAppendEntries();
          Coroutine::Sleep(HEARTBEAT_INTERVAL);
          break;
        case State::FOLLOWER:
          WaitForHeartbeat(election_timeout);
          break; 
        case State::CANDIDATE:
          StartElection();
          break; 
      }
    }
  }); 
}

bool RaftServer::Start(shared_ptr<Marshallable> &cmd,
                       uint64_t *index,
                       uint64_t *term) {
  /* Your code here. This function can be called from another OS thread. */
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  if ((state != State::LEADER) || (cmd == nullptr)) {
    mtx_.unlock(); 
    return false; 
  }
  logs.push_back(MarshallableLogEntry{cmd, current_term}); 
  match_index[loc_id_] = logs.size() - 1;
  *index = match_index[loc_id_]; 
  *term = current_term;           
  return true; 
}

void RaftServer::GetState(bool *is_leader, uint64_t *term) {
  /* Your code here. This function can be called from another OS thread. */
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  *is_leader = (state == State::LEADER); 
  *term = current_term;
}

void RaftServer::SyncRpcExample() {
  /* This is an example of synchronous RPC using coroutine; feel free to 
     modify this function to dispatch/receive your own messages. 
     You can refer to the other function examples in commo.h/cc on how 
     to send/recv a Marshallable object over RPC. */
  Coroutine::CreateRun([this](){
    string res;
    auto event = commo()->SendString(0, /* partition id is always 0 for lab1 */
                                     0, "hello", &res);
    event->Wait(1000000); //timeout after 1000000us=1s
    if (event->IsTimeout()) {
      Log_debug("timeout happens");
    } else {
      Log_debug("rpc response is: %s", res.c_str()); 
  } });
}

void RaftServer::UpdateTerm(uint64_t term) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  if (term > current_term) {
    // term increases monotonically
    current_term = term; 
    voted_for = -1; // do we need this?
    election_timeout = dist(rng);
  }
}

void RaftServer::SendAppendEntries() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto proxies = commo()->rpc_par_proxies_[partition_id_];

  // recieve append entries from previous round
  for (const auto& p : proxies) {
    if (append_events[p.first] != nullptr) {
      bool_t followerAppendOK = append_events[p.first]->IsReady() && append_events[p.first]->Get(); 
      if (followerAppendOK) {
        match_index[p.first] = next_index[p.first] - 1; 
      } else {
        next_index[p.first] = match_index[p.first] + 1; 
      } 
      append_events[p.first] = nullptr; 
    }
  }

  // find latest index that is replicated on majority of servers
  int next_commit_index = logs.size() - 1; 
  while (next_commit_index > commit_index && logs[next_commit_index].term == current_term) {
    int count = std::count_if(match_index.begin(), match_index.end(), 
                              [this, next_commit_index](uint64_t idx) { return idx >= next_commit_index; });
    if (count >= target_majority) {
      break; 
    }
    next_commit_index--; 
  }
  if (logs[next_commit_index].term != current_term) {
    // do not commit entries from previous terms
    next_commit_index = commit_index; 
  }
  
  // commit entries on leader node
  while (commit_index < next_commit_index) {
    app_next_(*logs[++commit_index].cmd);
    auto cmdptr = dynamic_pointer_cast<TpcCommitCommand>(logs[commit_index].cmd);
    Log_debug("Leader %d committed cmd %d at index %d in term %d", 
              loc_id_, cmdptr->tx_id_, commit_index, current_term);
  }

  // send append entries to proxies
  for (const auto& p : proxies) {
    if (p.first == loc_id_) {
      continue; 
    }
    if (next_index[p.first] < logs.size()) {
      std::vector<MarshallDeputyLogEntry> entries;
      std::transform(logs.begin() + next_index[p.first], logs.end(), std::back_inserter(entries), 
        [](const MarshallableLogEntry& entry) {
          MarshallDeputy md(entry.cmd); 
          return MarshallDeputyLogEntry{md, entry.term};
        });
      append_events[p.first] = commo()->SendAppendEntries(partition_id_, /* partition id is always 0 for lab1 */
                                                          p.first, current_term, loc_id_, 
                                                          next_index[p.first] - 1, logs[next_index[p.first] - 1].term, 
                                                          entries, commit_index);
      auto cmdptr = dynamic_pointer_cast<TpcCommitCommand>(logs[next_index[p.first]].cmd);
      Log_debug("Leader %d sending %d append entries for cmd %d at index %d to %d in term %d", 
                loc_id_, entries.size(), cmdptr->tx_id_, next_index[p.first], p.first, current_term);
      next_index[p.first] = logs.size(); 
    } else {
      commo()->SendEmptyAppendEntries(partition_id_, /* partition id is always 0 for lab1 */
                                      p.first, current_term, loc_id_, commit_index);
    }
  }
}

void RaftServer::StartElection() {
  mtx_.lock(); 
  UpdateTerm(current_term + 1);
  uint64_t ret_term; 
  auto ev = commo()->SendRequestVote(partition_id_, /* partition id is always 0 for lab1 */
                                    -1, current_term, loc_id_, logs.size() - 1, logs.back().term, &ret_term);
  mtx_.unlock(); 

  if (!ev->WaitUntilGreaterOrEqualThan(target_majority, election_timeout)) {
    mtx_.lock(); 
    state = State::LEADER;
    next_index.assign(num_proxies, logs.size());
    match_index.assign(num_proxies, 0);
    append_events.assign(num_proxies, nullptr);
    Log_debug("Server %d became leader for term %d", loc_id_, current_term);
    mtx_.unlock(); 
  } else {
    mtx_.lock(); 
    if (ret_term > current_term) {
      state = State::FOLLOWER; 
      UpdateTerm(ret_term);  
    }
    Log_debug("Server %d failed to become leader for term %d", loc_id_, current_term);
    mtx_.unlock(); 
  }
}

void RaftServer::WaitForHeartbeat(uint64_t timeout) {
  mtx_.lock(); 
  heartbeat_event = Reactor::CreateSpEvent<IntEvent>();
  mtx_.unlock(); 

  heartbeat_event->Wait(timeout); 

  mtx_.lock(); 
  if (heartbeat_event->IsTimeout()) {
    state = State::CANDIDATE;
  }
  heartbeat_event = nullptr;
  mtx_.unlock(); 
}

void RaftServer::HandleRequestVote(const uint64_t &term,
                                   const locid_t &candidate_id,
                                   const uint64_t &last_log_index,
                                   const uint64_t &last_log_term,
                                   uint64_t *ret_term,
                                   bool_t *vote_granted) {
  /* Your code here */
  std::lock_guard<std::recursive_mutex> lock(mtx_); 

  bool_t has_voted = voted_for != static_cast<locid_t>(-1); 
  bool_t up_to_date = last_log_term > logs.back().term || 
    (last_log_term == logs.back().term && last_log_index >= logs.size() - 1);

  *ret_term = current_term; 
  *vote_granted = false; // assume vote not granted by default
  if (term < current_term || (term == current_term && has_voted)) {
    return; 
  }

  if (term > current_term) {
    state = State::CANDIDATE; 
    UpdateTerm(term); 
    *ret_term = current_term; 
  }

  if (up_to_date) {
    state = State::FOLLOWER; 
    voted_for = candidate_id;   
    *vote_granted = true; 
  }
}

void RaftServer::HandleAppendEntries(const uint64_t &term,
                                     const locid_t &leader_id,
                                     const uint64_t &prev_log_index,
                                     const uint64_t &prev_log_term,
                                     const std::vector<MarshallableLogEntry> &entries,
                                     const uint64_t &leader_commit,
                                     bool_t *followerAppendOK) {
  /* Your code here */
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  bool_t log_matches = prev_log_index < logs.size() && logs[prev_log_index].term == prev_log_term; 
  if (term >= current_term && log_matches) {
    while (logs.size() > prev_log_index + 1) {
      logs.pop_back();
    }
    logs.insert(logs.end(), entries.begin(), entries.end());
    *followerAppendOK = true;  
    Log_debug("Follower %d appended %d entries from %d in term %d", 
              loc_id_, entries.size(), leader_id, current_term);
  } else {
    *followerAppendOK = false; 
    Log_debug("Follower %d rejected %d entries from %d in term %d", 
              loc_id_, entries.size(), leader_id, current_term);
  }
  HandleEmptyAppendEntries(term, leader_id, leader_commit); 
}

void RaftServer::HandleEmptyAppendEntries(const uint64_t &term,
                                          const locid_t &leader_id,
                                          const uint64_t &leader_commit) {
  /* Your code here */
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  if (term < current_term) {
    return; 
  }
  state = State::FOLLOWER; 
  UpdateTerm(term); 
  while (commit_index < leader_commit && commit_index < logs.size() - 1) {
    commit_index++; 
    app_next_(*logs[commit_index].cmd);
    auto cmdptr = dynamic_pointer_cast<TpcCommitCommand>(logs[commit_index].cmd);
    Log_debug("Follower %d committed cmd %d at index %d in term %d", 
              loc_id_, cmdptr->tx_id_, commit_index, current_term);
  }
  if (heartbeat_event != nullptr && !heartbeat_event->IsTimeout() && !heartbeat_event->IsReady()) {
    heartbeat_event->Set(1); 
  }
}

/* Do not modify any code below here */

void RaftServer::Disconnect(const bool disconnect) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(disconnected_ != disconnect);
  // global map of rpc_par_proxies_ values accessed by partition then by site
  static map<parid_t, map<siteid_t, map<siteid_t, vector<SiteProxyPair>>>> _proxies{};
  if (_proxies.find(partition_id_) == _proxies.end()) {
    _proxies[partition_id_] = {};
  }
  RaftCommo *c = (RaftCommo*) commo();
  if (disconnect) {
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() > 0);
    auto sz = c->rpc_par_proxies_.size();
    _proxies[partition_id_][loc_id_].insert(c->rpc_par_proxies_.begin(), c->rpc_par_proxies_.end());
    c->rpc_par_proxies_ = {};
    verify(_proxies[partition_id_][loc_id_].size() == sz);
    verify(c->rpc_par_proxies_.size() == 0);
  } else {
    verify(_proxies[partition_id_][loc_id_].size() > 0);
    auto sz = _proxies[partition_id_][loc_id_].size();
    c->rpc_par_proxies_ = {};
    c->rpc_par_proxies_.insert(_proxies[partition_id_][loc_id_].begin(), _proxies[partition_id_][loc_id_].end());
    _proxies[partition_id_][loc_id_] = {};
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() == sz);
  }
  disconnected_ = disconnect;
  if (disconnect) {
    Log_debug("Server %d disconnected", loc_id_);
  } else {
    Log_debug("Server %d reconnected", loc_id_);
  }
}

bool RaftServer::IsDisconnected() {
  return disconnected_;
}

} // namespace janus
