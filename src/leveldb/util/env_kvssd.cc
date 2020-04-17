// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"
#include "leveldb/status.h"
#include "port/port.h"
#include <string>

namespace leveldb {

class KVSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  void *mapped_region_;
  size_t length_;
  size_t offset_;

 public:
  KVSequentialFile(const std::string& fname, void *base, size_t length)
      : filename_(fname),mapped_region_(base), length_(length), offset_(0) {
  }

  virtual ~KVSequentialFile() {
    free(mapped_region_);
  }

  // n is aligned with 64B
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    if (offset_ + n > length_) { // return rest of the region
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset_, length_ - offset_);
      offset_ = length_;
      //s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset_, n);
      offset_ += n;
    }
    return s;
  }
  virtual Status Skip(uint64_t n) {
    offset_ += n;
    return Status::OK();
  }
};

class KVRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  void* mapped_region_;
  size_t length_;

 public:
  KVRandomAccessFile(const std::string& fname, void* base, size_t length)
      : filename_(fname), mapped_region_(base), length_(length) {
  }

  virtual ~KVRandomAccessFile() {
    free(mapped_region_);
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    if (offset + n > length_) { // return rest of the region
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset, length_ - offset);
      //s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset, n);
    }
    return s;
  }
};

class KVWritableFile : public WritableFile {
 private:
  std::string filename_;
  std::string value_;
  kvssd::KVSSD* kvd_;
  bool synced;

 public:
  KVWritableFile(kvssd::KVSSD* kvd, const std::string& fname)
      : filename_(fname), kvd_(kvd), synced(false) {  }

  ~KVWritableFile() { }

  virtual Status Append(const Slice& data) {
    value_.append(data.data(), data.size());
    return Status::OK();
  }

  virtual Status Reset() {
    value_.clear();
    return Status::OK();
  }

  virtual Status Close() {
    if (!synced) Sync();
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    //Status s = SyncDirIfManifest();
    Status s;
    kvssd::Slice key (filename_);
    kvssd::Slice val (value_);
    kvd_->kv_store(&key, &val);
    synced = true;
    //printf("KVWritable: %s, size %d bytes\n",filename_.c_str(), val.size());
    return s;
  }
};

class KVAppendableFile : public WritableFile {
 private:
  std::string filename_;
  std::string value_;
  kvssd::KVSSD* kvd_;

 public:
  KVAppendableFile(kvssd::KVSSD* kvd, const std::string& fname)
      : filename_(fname), kvd_(kvd){  }

  ~KVAppendableFile() { }

  virtual Status Append(const Slice& data) {
    value_.clear();
    value_.append(data.data(), data.size());
    return Status::OK();
  }

  virtual Status Close() {
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    //Status s = SyncDirIfManifest();
    Status s;
    kvssd::Slice key (filename_);
    kvssd::Slice val (value_);
    kvd_->kv_append(&key, &val);
    //printf("append: %s\n",filename_.c_str());
    return s;
  }
};

class KVSSDEnv : public EnvWrapper {
  private:
    kvssd::KVSSD* kvd_;
  public:
  explicit KVSSDEnv(Env* base_env, kvssd::KVSSD* kvd) 
  : EnvWrapper(base_env), kvd_(kvd) {
  }
  virtual ~KVSSDEnv() { }

  // Partial implementation of the Env interface.
  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    *result = NULL;
    char * base;
    int size;
    kvssd::Slice key (fname);
    kvd_->kv_get(&key, base, size);
    *result = new KVSequentialFile (fname, base, size);
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    char * base;
    int size;
    kvssd::Slice key (fname);
    kvd_->kv_get(&key, base, size);
    *result = new KVRandomAccessFile (fname, base, size);
    return Status::OK();
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    *result = new KVWritableFile(kvd_, fname);
    return Status::OK();
  }

  virtual Status NewAppendableFile(const std::string& fname,
                                   WritableFile** result) {
    *result = new KVAppendableFile(kvd_, fname);
    return Status::OK();
  }

  virtual bool FileExists(const std::string& fname) {
    kvssd::Slice key(fname);
    return kvd_->kv_exist(&key);
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();
    std::vector<std::string> keys;
    kvd_->kv_scan_keys(keys);

    for (auto it = keys.begin(); it != keys.end(); ++it){
      const std::string& filename = *it;
      if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' &&
          Slice(filename).starts_with(Slice(dir))) {
        result->push_back(filename.substr(dir.size() + 1));
      }
    }

    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    kvssd::Slice key(fname);
    if (!kvd_->kv_exist(&key)) {
      return Status::IOError(fname, "KV not found");
    }
    kvd_->kv_delete(&key);
    return Status::OK();
  }

  virtual Status CreateDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status DeleteDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) {
    kvssd::Slice key(fname);
    if (!kvd_->kv_exist(&key)) {
      return Status::IOError(fname, "KV not found");
    }

    *file_size = kvd_->kv_get_size(&key);
    return Status::OK();
  }

  // expensive, probably don't using it
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) {
    kvssd::Slice key_src(src);
    if (!kvd_->kv_exist(&key_src)) {
      return Status::IOError(src, "KV not found");
    }
    kvssd::Slice key_target(target);
    char *vbuf;
    int vlen;
    kvd_->kv_get(&key_src, vbuf, vlen);
    kvssd::Slice val_src (vbuf, vlen);
    kvd_->kv_store(&key_target, &val_src);
    kvd_->kv_delete(&key_src);

    return Status::OK();
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    return Status::OK();
  }

  virtual Status UnlockFile(FileLock* lock) {
    return Status::OK();
  }

  virtual Status GetTestDirectory(std::string* path) {
    *path = "/test";
    return Status::OK();
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    *result = NULL;
    return Status::OK();
  }
};


Env* NewKVEnv(Env* base_env, kvssd::KVSSD* kvd) {
  return new KVSSDEnv(base_env, kvd);
}

}  // namespace leveldb
