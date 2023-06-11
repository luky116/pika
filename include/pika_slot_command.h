#ifndef PIKA_SLOT_COMMAND_H_
#define PIKA_SLOT_COMMAND_H_

#include "include/pika_client_conn.h"
#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "net/include/net_cli.h"
#include "net/include/net_thread.h"
#include "storage/storage.h"
#include "strings.h"

const std::string SlotKeyPrefix = "_internal:slotkey:4migrate:";
const std::string SlotTagPrefix = "_internal:slottag:4migrate:";
const size_t MaxKeySendSize = 10 * 1024;
const int asyncRecvsNum = 64;

// crc32
#define HASH_SLOTS_MASK 0x000003ff
#define HASH_SLOTS_SIZE (HASH_SLOTS_MASK + 1)

extern uint32_t crc32tab[256];

void CRC32TableInit(uint32_t poly);

extern void InitCRC32Table();

extern uint32_t CRC32Update(uint32_t crc, const char *buf, int len);
extern uint32_t CRC32CheckSum(const char *buf, int len);

int GetSlotID(const std::string &str);
int GetKeyType(const std::string key, std::string &key_type, std::shared_ptr<Slot> slot);
void AddSlotKey(const std::string type, const std::string key, std::shared_ptr<Slot> slot);
void RemKeyNotExists(const std::string type, const std::string key, std::shared_ptr<Slot> slot);
void RemSlotKey(const std::string key, std::shared_ptr<Slot> slot);
int DeleteKey(const std::string key, const char key_type, std::shared_ptr<Slot> slot);
std::string GetSlotKey(int slot);
std::string GetSlotsTagKey(uint32_t crc);
int GetSlotsID(const std::string &str, uint32_t *pcrc, int *phastag);
void SlotKeyRemByType(const std::string &type, const std::string &key, std::shared_ptr<Slot> slot);

class PikaMigrate {
 public:
  PikaMigrate();
  virtual ~PikaMigrate();

  int MigrateKey(const std::string &host, const int port, int db, int timeout, const std::string &key, const char type,
                 std::string &detail, std::shared_ptr<Slot> slot);
  void CleanMigrateClient();

  void Lock() {
    LOG(INFO) << "migrate lock";
    mutex_.lock();
  }
  int Trylock() {
    LOG(INFO) << "migrate trylock";
    return mutex_.try_lock();
  }
  void Unlock() {
    LOG(INFO) << "migrate unlock";
    mutex_.unlock();
  }
  net::NetCli *GetMigrateClient(const std::string &host, const int port, int timeout);

 private:
  std::map<std::string, void *> migrate_clients_;
  pstd::Mutex mutex_;

  void KillMigrateClient(net::NetCli *migrate_cli);
  void KillAllMigrateClient();

  int MigrateSend(net::NetCli *migrate_cli, const std::string &key, const char type, std::string &detail,
                  std::shared_ptr<Slot> slot);
  bool MigrateRecv(net::NetCli *migrate_cli, int need_receive, std::string &detail);

  int ParseKey(const std::string &key, const char type, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  int64_t TTLByType(const char key_type, const std::string &key, std::shared_ptr<Slot> slot);
  int ParseKKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  int ParseZKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  int ParseSKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  int ParseHKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  int ParseLKey(const std::string &key, std::string &wbuf_str, std::shared_ptr<Slot> slot);
  bool SetTTL(const std::string &key, std::string &wbuf_str, int64_t ttl);
};

class SlotsMgrtTagSlotCmd : public Cmd {
 public:
  SlotsMgrtTagSlotCmd(const std::string &name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Slot> slot);
  virtual void Split(std::shared_ptr<Slot> slot, const HintKeys &hint_keys){};
  virtual void Merge(){};
  virtual Cmd *Clone() override { return new SlotsMgrtTagSlotCmd(*this); }

 private:
  std::string dest_ip_;
  int64_t dest_port_;
  int64_t timeout_ms_;
  int64_t slot_id_;
  std::basic_string<char, std::char_traits<char>, std::allocator<char>> key_;
  char key_type_;

  virtual void DoInitial();
  int SlotKeyPop(std::shared_ptr<Slot> slot);
};

class SlotsMgrtTagSlotAsyncCmd : public Cmd {
 public:
  SlotsMgrtTagSlotAsyncCmd(const std::string &name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Slot> slot);
  virtual void Split(std::shared_ptr<Slot> slot, const HintKeys &hint_keys){};
  virtual void Merge(){};
  virtual Cmd *Clone() override { return new SlotsMgrtTagSlotAsyncCmd(*this); }

 private:
  std::string dest_ip_;
  int64_t dest_port_;
  int64_t timeout_ms_;
  int64_t max_bulks_;
  int64_t max_bytes_;
  int64_t slot_num_;
  int64_t keys_num_;

  virtual void DoInitial();
};

class SlotsMgrtTagOneCmd : public Cmd {
 public:
  SlotsMgrtTagOneCmd(const std::string &name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Slot> slot);
  virtual void Split(std::shared_ptr<Slot> slot, const HintKeys &hint_keys){};
  virtual void Merge(){};
  virtual Cmd *Clone() override { return new SlotsMgrtTagOneCmd(*this); }

 private:
  std::string dest_ip_;
  int64_t dest_port_;
  int64_t timeout_ms_;
  std::string key_;
  int64_t slot_num_;
  char key_type_;
  virtual void DoInitial();
  int KeyTypeCheck(std::shared_ptr<Slot> slot);
  int SlotKeyRemCheck(std::shared_ptr<Slot> slot);
};

class SlotsMgrtAsyncStatusCmd : public Cmd {
 public:
  SlotsMgrtAsyncStatusCmd(const std::string &name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Slot> slot = nullptr);
  virtual void Split(std::shared_ptr<Slot> slot, const HintKeys &hint_keys){};
  virtual void Merge(){};
  virtual Cmd *Clone() override { return new SlotsMgrtAsyncStatusCmd(*this); }

 private:
  virtual void DoInitial() override;
};

class SlotsInfoCmd : public Cmd {
 public:
  SlotsInfoCmd(const std::string &name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Slot> slot);
  virtual void Split(std::shared_ptr<Slot> slot, const HintKeys &hint_keys){};
  virtual void Merge(){};
  virtual Cmd *Clone() override { return new SlotsInfoCmd(*this); }

 private:
  virtual void DoInitial();
};

class SlotsMgrtAsyncCancelCmd : public Cmd {
 public:
  SlotsMgrtAsyncCancelCmd(const std::string &name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Slot> slot);
  virtual void Split(std::shared_ptr<Slot> slot, const HintKeys &hint_keys){};
  virtual void Merge(){};
  virtual Cmd *Clone() override { return new SlotsMgrtAsyncCancelCmd(*this); }

 private:
  virtual void DoInitial();
};

class SlotsDelCmd : public Cmd {
 public:
  SlotsDelCmd(const std::string &name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Slot> slot);
  virtual void Split(std::shared_ptr<Slot> slot, const HintKeys &hint_keys){};
  virtual void Merge(){};
  virtual Cmd *Clone() override { return new SlotsDelCmd(*this); }

 private:
  std::vector<std::string> slots_;
  virtual void DoInitial();
};

class SlotsHashKeyCmd : public Cmd {
 public:
  SlotsHashKeyCmd(const std::string &name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Slot> slot);
  virtual void Split(std::shared_ptr<Slot> slot, const HintKeys &hint_keys){};
  virtual void Merge(){};
  virtual Cmd *Clone() override { return new SlotsHashKeyCmd(*this); }

 private:
  std::vector<std::string> keys_;
  virtual void DoInitial();
};

#endif
