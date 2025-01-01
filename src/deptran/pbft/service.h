#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "pbft_rpc.h"
#include "server.h"
#include "macros.h"

class SimpleCommand;
namespace janus {

class TxLogServer;
class PbftServer;
class PbftServiceImpl : public PbftService {
 public:
  PbftServer* svr_;
  PbftServiceImpl(TxLogServer* sched);

  RpcHandler(Preprepare, 2, const PreprepareRequest&, req, const Message&, mesg) { }
  RpcHandler(Prepare, 1, const PrepareRequest&, req) { }
  RpcHandler(Commit, 1, const CommitRequest&, req) { }

  RpcHandler(Prepared, 1, const PreparedRequest&, req) { }  
  RpcHandler(Committed, 1, const CommittedRequest&, req) { }
            
  RpcHandler(Checkpoint, 1, const CheckpointRequest&, req) { }

  RpcHandler(ViewChange, 1, const ViewChangeRequest&, req) { }
  RpcHandler(NewView, 1, const NewViewRequest&, req) { }
};

} // namespace janus
