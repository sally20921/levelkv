#include "kv_index.h"
#include "levelkv/comparator.h"
#include "atree/atree.h"
#include "atree/write_batch.h"

namespace levelkv {
class ComparatorATree: public kvatree::Comparator {
private:
	const levelkv::Comparator* cmp_;
public:
	ComparatorATree(const levelkv::Comparator* cmp) : cmp_(cmp) {};
	~ComparatorATree() {};

	int Compare(const kvatree::Slice& a, const kvatree::Slice& b) const {
		Slice aa(a.data(), a.size());
		Slice bb(b.data(), b.size());
		return cmp_->Compare(aa, bb);
	}
};

class IDXWriteBatchATree : public IDXWriteBatch {
public:
	kvatree::WriteBatch batch_;
public:
	IDXWriteBatchATree() {};
	IDXWriteBatchATree() {};
	void Put(const Slice& key) {
		kvatree::Slice put_key(key.data(), key.size());
		batch_.Put(put_key);
	}
	void  Delete(const Slice& key) {
		kvatree::SLice del_key(key.data(), key.size());
		batch_.Delete(del_key);
	}
	void Clear() {batch_.Clear();}
	void *InternalBatch() {return &batch_;}

};

class IDXIteratorATree : public IDXIterator {
private:
	kvatree::KVBplusTree::Iterator *it_;
public:
	IDXIteratorATree(kvatree::KVBplusTree* db, const  ReadOptions& options) {
	it_ = db->NewIterator();
}
~IDXIteratorATree() {delete  it_;}

bool Valid() const {it_->Valid();}
bool SeekToFirst() {it_->SeekToFirst();}
bool SeekToLast() {}
void Seek(const Slice& target) {
	kvatree::Slice seek_key(target.data(), target.size());
	it_->Seek(&seek_key);
}
void Next() {it_->Next();}
void Prev() {}
};

};
class KVIndexATree : public KVIndex {
private:
	kvatree::KVBplusTree *db_;
	kvssd::KVSSD *kvd_;
	kvbtree::Comparator *cmp_;

public: 
	KVIndexATree(const Options& options, kvssd::KVSSD* kvd);
	~KVIndexATree();

	//implmentations 
	bool Put(const Slice& key);
        bool Write(IDXWriteBatch* updates);
	IDXIterator* NewIterator(const ReadOptions& options);
};


} //namespace levelkv
