#pragma once

#include "testconf.h"

namespace janus {

#ifdef PBFT_TEST_CORO

class PbftLabTest {

 private:
  PbftTestConfig *config_;
  uint64_t index_;
  uint64_t init_rpcs_;

 public:
  PbftLabTest(PbftTestConfig *config) : config_(config), index_(1) {}
  int Run(void);
  void Cleanup(void);

 private:

  int testBasicAgree(void);

  void wait(uint64_t microseconds);

};

#endif

} // namespace janus
