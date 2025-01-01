
#include "commo.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "pbft_rpc.h"
#include "macros.h"
#include "server.h"

namespace janus {

PbftCommo::PbftCommo(PollMgr* poll) : Communicator(poll) {
}

void
PbftCommo::SendPreprepare(parid_t par_id,
                          siteid_t site_id,
                          const PreprepareRequest& req,
                          const Message& cmd) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id || site_id == static_cast<siteid_t>(-1)) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, Preprepare, req, cmd, fuattr);
    }
  }
}

void
PbftCommo::SendPrepare(parid_t par_id,
                       siteid_t site_id,
                       const PrepareRequest& req) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id || site_id == static_cast<siteid_t>(-1)) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, Prepare, req, fuattr);
    }
  }
}

void 
PbftCommo::SendCommit(parid_t par_id,
                      siteid_t site_id,
                      const CommitRequest& req) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id || site_id == static_cast<siteid_t>(-1)) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, Commit, req, fuattr);
    }
  }
}

void
PbftCommo::SendPrepared(parid_t par_id,
                        siteid_t site_id,
                        const PreparedRequest& req) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id || site_id == static_cast<siteid_t>(-1)) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, Prepared, req, fuattr);
    }
  }
}

void
PbftCommo::SendCommitted(parid_t par_id,
                         siteid_t site_id,
                         const CommittedRequest& req) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id || site_id == static_cast<siteid_t>(-1)) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, Committed, req, fuattr);
    }
  }
}

void
PbftCommo::SendCheckpoint(parid_t par_id,
                          siteid_t site_id,
                          const CheckpointRequest& req) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id || site_id == static_cast<siteid_t>(-1)) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, Checkpoint, req, fuattr);
    }
  }
}

void
PbftCommo::SendViewChange(parid_t par_id,
                          siteid_t site_id,
                          const ViewChangeRequest& req) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id || site_id == static_cast<siteid_t>(-1)) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, ViewChange, req, fuattr);
    }
  }
}

void
PbftCommo::SendNewView(parid_t par_id,
                       siteid_t site_id,
                       const NewViewRequest& req) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id || site_id == static_cast<siteid_t>(-1)) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, NewView, req, fuattr);
    }
  }
}


} // namespace janus
