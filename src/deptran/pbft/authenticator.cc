#include <openssl/rsa.h>

#include "authenticator.h"

namespace janus {

Authenticator::Authenticator(const EVP_MD *md, const std::shared_ptr<EVP_PKEY> &key) 
    : key_(key) {
  verify((mdctx_ = EVP_MD_CTX_new()) != NULL);
  verify(EVP_DigestInit_ex(mdctx_, md, NULL) == 1);
  verify((keyctx_ = EVP_PKEY_CTX_new(key.get(), NULL)) != NULL);
}

Authenticator::~Authenticator() {
  if (mdctx_ != NULL) {
    EVP_MD_CTX_free(mdctx_);
  }
  if (keyctx_ != NULL) {
    EVP_PKEY_CTX_free(keyctx_);
  }
}

std::string Authenticator::SignHash(const std::string &hash) {
  size_t siglen; 
  unsigned char *raw_sig; 
  if (EVP_PKEY_sign_init(keyctx_) != 1) {
    Log_fatal("EVP_PKEY_sign_init failed");
  }
  if (EVP_PKEY_CTX_set_rsa_padding(keyctx_, RSA_PKCS1_PADDING) != 1) {
    Log_fatal("EVP_PKEY_CTX_set_rsa_padding failed");
  }
  if (EVP_PKEY_sign(keyctx_, NULL, &siglen, 
                    (const unsigned char *)hash.data(), hash.size()) != 1) {
    Log_fatal("EVP_PKEY_sign failed");
  }
  if ((raw_sig = (unsigned char *)OPENSSL_malloc(siglen)) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_PKEY_sign(keyctx_, raw_sig, &siglen, 
                    (const unsigned char *)hash.data(), hash.size()) != 1) {
    Log_fatal("EVP_PKEY_sign failed");
  }
  std::string signature (raw_sig, raw_sig + siglen);
  OPENSSL_free(raw_sig); 
  return signature; 
}

bool Authenticator::VerifyHash(const std::string &hash, const std::string &signature) {
  if (EVP_PKEY_verify_init(keyctx_) != 1) {
    Log_fatal("EVP_PKEY_verify_init failed");
  }
  if (EVP_PKEY_CTX_set_rsa_padding(keyctx_, RSA_PKCS1_PADDING) != 1) {
    Log_fatal("EVP_PKEY_CTX_set_rsa_padding failed");
  }
  int ret = EVP_PKEY_verify(keyctx_, 
                            (const unsigned char *)signature.data(), signature.size(),
                            (const unsigned char *)hash.data(), hash.size());
  return ret == 1;
}

std::string Authenticator::GetDigest(Marshallable &cmd) {
  std::string cmd_str = cmd.ToString(); 
  unsigned int hash_len = EVP_MD_size(EVP_MD_CTX_get0_md(mdctx_));
  unsigned char *raw_digest; 
  if (EVP_DigestInit_ex(mdctx_, NULL, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, cmd_str.data(), cmd_str.size()) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_digest = (unsigned char *)OPENSSL_malloc(hash_len)) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_digest, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string digest (raw_digest, raw_digest + hash_len); 
  OPENSSL_free(raw_digest);
  return digest; 
}

std::string Authenticator::GetChkptDigest(const std::string &chkpt_digest, Marshallable &cmd) {
  std::string cmd_digest = GetDigest(cmd); 
  std::vector<unsigned char> bytes (cmd_digest.begin(), cmd_digest.end()); 
  for (int i = 0; i < chkpt_digest.size(); i++) {
    bytes[i] ^= chkpt_digest[i]; 
  }
  std::string new_chkpt_digest (bytes.begin(), bytes.end()); 
  return new_chkpt_digest; 
}

std::string Authenticator::GetRequestHash(const std::shared_ptr<Marshallable> &cmd, const Request &req) {
  unsigned int hash_len = EVP_MD_size(EVP_MD_CTX_get0_md(mdctx_));
  unsigned char *raw_hash; 
  std::string cmd_str = cmd->ToString();
  if (EVP_DigestInit_ex(mdctx_, NULL, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, cmd_str.data(), cmd_str.size()) != 1 ||
      EVP_DigestUpdate(mdctx_, &req.timestamp, sizeof(req.timestamp)) != 1 ||
      EVP_DigestUpdate(mdctx_, &req.client_id, sizeof(req.client_id)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(hash_len)) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string digest (raw_hash, raw_hash + hash_len); 
  OPENSSL_free(raw_hash);
  return digest; 
}

std::string Authenticator::GetPreprepareHash(const PreprepareMessage &mesg) {
  unsigned int hash_len = EVP_MD_size(EVP_MD_CTX_get0_md(mdctx_));
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, NULL, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.view, sizeof(mesg.view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.seqno, sizeof(mesg.seqno)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(hash_len)) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string hash (raw_hash, raw_hash + hash_len); 
  OPENSSL_free(raw_hash);
  return hash; 
}

std::string Authenticator::GetPrepareHash(const PrepareMessage &mesg) {
  unsigned int hash_len = EVP_MD_size(EVP_MD_CTX_get0_md(mdctx_));
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, NULL, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.view, sizeof(mesg.view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.seqno, sizeof(mesg.seqno)) != 1 ||
      EVP_DigestUpdate(mdctx_, mesg.digest.data(), mesg.digest.size()) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(hash_len)) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string hash (raw_hash, raw_hash + hash_len); 
  OPENSSL_free(raw_hash);
  return hash; 
}

std::string Authenticator::GetCommitHash(const CommitMessage &mesg) {
  unsigned int hash_len = EVP_MD_size(EVP_MD_CTX_get0_md(mdctx_));
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, NULL, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.view, sizeof(mesg.view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.seqno, sizeof(mesg.seqno)) != 1 ||
      EVP_DigestUpdate(mdctx_, mesg.digest.data(), mesg.digest.size()) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(hash_len)) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string hash (raw_hash, raw_hash + hash_len); 
  OPENSSL_free(raw_hash);
  return hash; 
}

std::string Authenticator::GetCheckpointHash(const CheckpointMessage &mesg) {
  unsigned int hash_len = EVP_MD_size(EVP_MD_CTX_get0_md(mdctx_));
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, NULL, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.chkpt_seqno, sizeof(mesg.chkpt_seqno)) != 1 ||
      EVP_DigestUpdate(mdctx_, mesg.chkpt_digest.data(), mesg.chkpt_digest.size()) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(hash_len)) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string hash (raw_hash, raw_hash + hash_len); 
  OPENSSL_free(raw_hash);
  return hash; 
}

std::string Authenticator::GetViewChangeHash(const ViewChangeMessage &mesg) {
  unsigned int hash_len = EVP_MD_size(EVP_MD_CTX_get0_md(mdctx_));
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, NULL, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  } 
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.new_view, sizeof(mesg.new_view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.chkpt_seqno, sizeof(mesg.chkpt_seqno)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  for (const auto &chkpt_entry : mesg.checkpoints) {
    const CheckpointMessage &chkpt = chkpt_entry.second; 
    if (EVP_DigestUpdate(mdctx_, chkpt.chkpt_digest.data(), chkpt.chkpt_digest.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    } 
  }
  for (const auto &preprepare_entry : mesg.preprepares) {
    const PreprepareMessage &preprepare = preprepare_entry.second; 
    if (EVP_DigestUpdate(mdctx_, preprepare.digest.data(), preprepare.digest.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    } 
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(hash_len)) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string hash (raw_hash, raw_hash + hash_len); 
  OPENSSL_free(raw_hash);
  return hash; 
}

std::string Authenticator::GetNewViewHash(const NewViewMessage &mesg) {
  unsigned int hash_len = EVP_MD_size(EVP_MD_CTX_get0_md(mdctx_));
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, NULL, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  } 
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.new_view, sizeof(mesg.new_view)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  for (const auto &view_change_entry : mesg.view_changes) {
    const ViewChangeMessage &view_change = view_change_entry.second; 
    if (EVP_DigestUpdate(mdctx_, view_change.signature.data(), view_change.signature.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    } 
  }
  for (const auto &preprepare_entry : mesg.preprepares) {
    const PreprepareMessage &preprepare = preprepare_entry.second; 
    if (EVP_DigestUpdate(mdctx_, preprepare.digest.data(), preprepare.digest.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    } 
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(hash_len)) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string hash (raw_hash, raw_hash + hash_len); 
  OPENSSL_free(raw_hash);
  return hash; 
}

std::string Authenticator::GetReplyHash(const Reply &rep) {
  unsigned int hash_len = EVP_MD_size(EVP_MD_CTX_get0_md(mdctx_));
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, NULL, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &rep.view, sizeof(rep.view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &rep.timestamp, sizeof(rep.timestamp)) != 1 ||
      EVP_DigestUpdate(mdctx_, &rep.client_id, sizeof(rep.client_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &rep.server_id, sizeof(rep.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, rep.reply.data(), rep.reply.size()) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(hash_len)) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string hash (raw_hash, raw_hash + hash_len); 
  OPENSSL_free(raw_hash);
  return hash;  
}

void Authenticator::SignRequest(const std::shared_ptr<Marshallable> &cmd, Request &req) {
  std::string hash = GetRequestHash(cmd, req); 
  req.signature = SignHash(hash);
}

void Authenticator::SignPreprepare(PreprepareMessage &mesg) {
  std::string hash = GetPreprepareHash(mesg); 
  mesg.signature = SignHash(hash);
}

void Authenticator::SignPrepare(PrepareMessage &mesg) {
  std::string hash = GetPrepareHash(mesg); 
  mesg.signature = SignHash(hash);
}

void Authenticator::SignCommit(CommitMessage &mesg) {
  std::string hash = GetCommitHash(mesg); 
  mesg.signature = SignHash(hash);
}

void Authenticator::SignCheckpoint(CheckpointMessage &mesg) {
  std::string hash = GetCheckpointHash(mesg); 
  mesg.signature = SignHash(hash);
}

void Authenticator::SignViewChange(ViewChangeMessage &mesg) {
  std::string hash = GetViewChangeHash(mesg); 
  mesg.signature = SignHash(hash);
}

void Authenticator::SignNewView(NewViewMessage &mesg) {
  std::string hash = GetNewViewHash(mesg); 
  mesg.signature = SignHash(hash);
}

void Authenticator::SignReply(Reply &rep) {
  std::string hash = GetReplyHash(rep); 
  rep.signature = SignHash(hash); 
}

bool Authenticator::VerifyRequest(const std::shared_ptr<Marshallable> &cmd, const Request &req) {
  std::string hash = GetRequestHash(cmd, req); 
  return VerifyHash(hash, req.signature);
}

bool Authenticator::VerifyPreprepare(const PreprepareMessage &mesg) {
  std::string hash = GetPreprepareHash(mesg); 
  return VerifyHash(hash, mesg.signature); 
}

bool Authenticator::VerifyPrepare(const PrepareMessage &mesg) {
  std::string hash = GetPrepareHash(mesg); 
  return VerifyHash(hash, mesg.signature); 
}

bool Authenticator::VerifyCommit(const CommitMessage &mesg) {
  std::string hash = GetCommitHash(mesg); 
  return VerifyHash(hash, mesg.signature); 
}

bool Authenticator::VerifyCheckpoint(const CheckpointMessage &mesg) {
  std::string hash = GetCheckpointHash(mesg); 
  return VerifyHash(hash, mesg.signature); 
}

bool Authenticator::VerifyViewChange(const ViewChangeMessage &mesg) {
  for (const auto &chkpt_entry : mesg.checkpoints) {
    const CheckpointMessage &chkpt = chkpt_entry.second;
    if (!VerifyCheckpoint(chkpt)) {
      return false; 
    }
  }
  for (const auto &preprepare_entry : mesg.preprepares) {
    const PreprepareMessage &preprepare = preprepare_entry.second;
    if (!VerifyPreprepare(preprepare)) {
      return false; 
    }
  }
  for (const auto &prepare_map : mesg.prepares) {
    for (const auto &prepare_entry : prepare_map.second) {
      const PrepareMessage &prepare = prepare_entry.second;
      if (!VerifyPrepare(prepare)) {
        return false; 
      }
    }
  }
  std::string hash = GetViewChangeHash(mesg);
  return VerifyHash(hash, mesg.signature);
}

bool Authenticator::VerifyNewView(const NewViewMessage &mesg) {
  for (const auto &view_change_entry : mesg.view_changes) {
    const ViewChangeMessage &view_change = view_change_entry.second; 
    if (!VerifyViewChange(view_change)) {
      return false; 
    }
  }
  std::string hash = GetNewViewHash(mesg); 
  return VerifyHash(hash, mesg.signature); 
}

bool Authenticator::VerifyReply(const Reply &rep) {
  std::string hash = GetReplyHash(rep); 
  return VerifyHash(hash, rep.signature); 
}

} // namespace janus