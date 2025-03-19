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

  RpcHandler(Preprepare, 4, 
            const PreprepareMessage&, mesg,
            const MarshallDeputy&, md_cmd,
            const uint64_t&, timestamp,
            const cliid_t&, client_id) { }
  RpcHandler(Prepare, 1, const PrepareMessage&, mesg) { }
  RpcHandler(Commit, 1, const CommitMessage&, mesg) { }

  RpcHandler(Checkpoint, 1, const CheckpointMessage&, mesg) { }

  RpcHandler(ViewChange, 1, const ViewChangeMessage&, mesg) { }
  RpcHandler(NewView, 1, const NewViewMessage&, mesg) { }
};

} // namespace janus
