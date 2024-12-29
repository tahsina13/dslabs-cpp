
#include "commo.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "raft_rpc.h"
#include "macros.h"
#include "server.h"

namespace janus {

RaftCommo::RaftCommo(PollMgr* poll) : Communicator(poll) {
}

std::shared_ptr<SharedIntEvent>
RaftCommo::SendRequestVote(parid_t par_id,
                           siteid_t site_id, // -1 for broadcast
                           uint64_t term,
                           locid_t candidate_id,
                           uint64_t last_log_index,
                           uint64_t last_log_term,
                           uint64_t *ret_term) {
  /*
   * Example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  auto proxies = rpc_par_proxies_[par_id];
  auto votes = std::make_shared<int>(0);
  auto ev = make_shared<SharedIntEvent>(); 
  if (ret_term != nullptr) {
    *ret_term = 0; 
  }
  for (auto& p : proxies) {
    if (p.first == site_id || site_id == static_cast<siteid_t>(-1)) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [votes, ev, ret_term](Future* fu) {
        /* this is a handler that will be invoked when the RPC returns */
        uint64_t term; 
        bool_t vote_granted;
        /* retrieve RPC return values in order */
        fu->get_reply() >> term;
        fu->get_reply() >> vote_granted;
        /* process the RPC response here */
        if (ret_term != nullptr) {
          *ret_term = max(*ret_term, term);  
        }
        if (vote_granted) {
          (*votes)++; 
          ev->Set(*votes); 
        }
      };
      /* Always use Call_Async(proxy, RPC name, RPC args..., fuattr)
      * to asynchronously invoke RPCs */
      Call_Async(proxy, RequestVote, term, candidate_id, last_log_index, last_log_term, fuattr);
    }
  }
  return ev; 
}

std::shared_ptr<BoxEvent<bool_t>>
RaftCommo::SendAppendEntries(parid_t par_id,
                             siteid_t site_id, // no broadcast
                             uint64_t term,
                             locid_t leader_id,
                             uint64_t prev_log_index,
                             uint64_t prev_log_term,
                             const std::vector<MarshallDeputyLogEntry>& entries,
                             uint64_t leader_commit) {
  /*
   * More example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  auto proxies = rpc_par_proxies_[par_id];
  auto ev = Reactor::CreateSpEvent<BoxEvent<bool_t>>();
  for (auto& p : proxies) {
    if (p.first == site_id) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [ev](Future* fu) {
        bool_t followerAppendOK;
        fu->get_reply() >> followerAppendOK;
        ev->Set(followerAppendOK); 
      };
      /* wrap Marshallable in a MarshallDeputy to send over RPC */
      Call_Async(proxy, AppendEntries, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit, fuattr);
    }
  }
  return ev; 
}

void
RaftCommo::SendEmptyAppendEntries(parid_t par_id,
                                  siteid_t site_id, // no broadcast 
                                  uint64_t term,
                                  locid_t leader_id,
                                  uint64_t leader_commit) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, EmptyAppendEntries, term, leader_id, leader_commit, fuattr);
    }
  }
}

std::shared_ptr<IntEvent> 
RaftCommo::SendString(parid_t par_id, siteid_t site_id, const string& msg, string* res) {
  auto proxies = rpc_par_proxies_[par_id];
  auto ev = Reactor::CreateSpEvent<IntEvent>();
  for (auto& p : proxies) {
    if (p.first == site_id) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [res,ev](Future* fu) {
        fu->get_reply() >> *res;
        ev->Set(1);
      };
      /* wrap Marshallable in a MarshallDeputy to send over RPC */
      Call_Async(proxy, HelloRpc, msg, fuattr);
    }
  }
  return ev;
}


} // namespace janus
