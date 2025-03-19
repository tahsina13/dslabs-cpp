
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
                          const PreprepareMessage& mesg,
                          const shared_ptr<Marshallable>& cmd,
                          uint64_t timestamp,
                          cliid_t client_id) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      MarshallDeputy md_cmd (cmd); 
      Call_Async(proxy, Preprepare, mesg, md_cmd, timestamp, client_id, fuattr);
    }
  }
}

void
PbftCommo::SendPrepare(parid_t par_id,
                       siteid_t site_id,
                       const PrepareMessage& mesg) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, Prepare, mesg, fuattr);
    }
  }
}

void 
PbftCommo::SendCommit(parid_t par_id,
                      siteid_t site_id,
                      const CommitMessage& mesg) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, Commit, mesg, fuattr);
    }
  }
}

void
PbftCommo::SendCheckpoint(parid_t par_id,
                          siteid_t site_id,
                          const CheckpointMessage& mesg) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, Checkpoint, mesg, fuattr);
    }
  }
}

void
PbftCommo::SendViewChange(parid_t par_id,
                          siteid_t site_id,
                          const ViewChangeMessage& mesg) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, ViewChange, mesg, fuattr);
    }
  }
}

void
PbftCommo::SendNewView(parid_t par_id,
                       siteid_t site_id,
                       const NewViewMessage& mesg) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      PbftProxy *proxy = (PbftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        /* do nothing */
      };
      Call_Async(proxy, NewView, mesg, fuattr);
    }
  }
}


} // namespace janus
