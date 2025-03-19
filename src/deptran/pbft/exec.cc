#include "exec.h"

namespace janus {


ballot_t PbftExecutor::Prepare(const ballot_t ballot) {
  verify(0);
  return 0;
}

ballot_t PbftExecutor::Accept(const ballot_t ballot,
                                    shared_ptr<Marshallable> cmd) {
  verify(0);
  return 0;
}

ballot_t PbftExecutor::AppendEntries(const ballot_t ballot,
                                         shared_ptr<Marshallable> cmd) {
  verify(0);
  return 0;
}

ballot_t PbftExecutor::Decide(ballot_t ballot, CmdData& cmd) {
  verify(0);
  return 0;
}

} // namespace janus
