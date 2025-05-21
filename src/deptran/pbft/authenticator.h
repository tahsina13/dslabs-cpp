#pragma once

#include <openssl/evp.h>

#include "../__dep__.h"
#include "../constants.h"
#include "../classic/tpc_command.h"
#include "pbft_rpc.h"

namespace janus {

class Authenticator {
  private: 
    EVP_MD_CTX *mdctx_; 
    std::shared_ptr<EVP_PKEY> key_; 
    EVP_PKEY_CTX *keyctx_; 
    
  public: 
    Authenticator(const EVP_MD *md, const std::shared_ptr<EVP_PKEY> &key);
    ~Authenticator();

  private: 
    std::string SignHash(const std::string &hash); 
    bool VerifyHash(const std::string &hash, const std::string &signature);

    std::string GetRequestHash(const std::shared_ptr<Marshallable> &cmd, const Request &req); 
    std::string GetPreprepareHash(const PreprepareMessage &mesg);
    std::string GetPrepareHash(const PrepareMessage &mesg);
    std::string GetCommitHash(const CommitMessage &mesg);
    std::string GetCheckpointHash(const CheckpointMessage &mesg);
    std::string GetViewChangeHash(const ViewChangeMessage &mesg);
    std::string GetNewViewHash(const NewViewMessage &mesg);
    std::string GetReplyHash(const Reply &rep); 

  public: 
    std::string GetDigest(Marshallable &cmd);
    std::string GetChkptDigest(const std::string &chkpt_digest, Marshallable &cmd); 
    
    void SignRequest(const std::shared_ptr<Marshallable> &cmd, Request &req);
    void SignPreprepare(PreprepareMessage &mesg); 
    void SignPrepare(PrepareMessage &mesg);
    void SignCommit(CommitMessage &mesg);
    void SignCheckpoint(CheckpointMessage &mesg);
    void SignViewChange(ViewChangeMessage &mesg);
    void SignNewView(NewViewMessage &mesg);
    void SignReply(Reply &rep); 

    bool VerifyRequest(const std::shared_ptr<Marshallable> &cmd, const Request &req); 
    bool VerifyPreprepare(const PreprepareMessage &mesg);
    bool VerifyPrepare(const PrepareMessage &mesg); 
    bool VerifyCommit(const CommitMessage &mesg);
    bool VerifyCheckpoint(const CheckpointMessage &mesg);
    bool VerifyViewChange(const ViewChangeMessage &mesg); 
    bool VerifyNewView(const NewViewMessage &mesg);
    bool VerifyReply(const Reply &rep); 
}; 

} // namespace janus