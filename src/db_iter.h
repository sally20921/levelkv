/******* levelkv *******/
/* db_iter.h
* 08/06/2019
* 
*/

#ifndef _levelkv_db_iter_h_
#define _levelkv_db_iter_h_

#include "levelkv/iterator.h"
#include "db_impl.h"

namespace levelkv {

Iterator* NewDBIterator(DBImpl *db, const ReadOptions &options);
} // end namespace levelkv

#endif