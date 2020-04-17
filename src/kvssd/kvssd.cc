#include "kvssd/kvssd.h"
#define ITER_BUFF 32768
#define INIT_GET_BUFF 65536 // 64KB

namespace kvssd {

  void on_io_complete(kvs_callback_context* ioctx) {
    if (ioctx->result == KVS_ERR_CONT_NOT_EXIST) {
      int test = 1;
    }
    if (ioctx->result != 0 && ioctx->result != KVS_ERR_KEY_NOT_EXIST) {
      printf("io error: op = %d, key = %s, result = 0x%x, err = %s\n", ioctx->opcode, ioctx->key ? (char*)ioctx->key->key:0, ioctx->result, kvs_errstr(ioctx->result));
      exit(1);
    }
    
    switch (ioctx->opcode) {
    case IOCB_ASYNC_PUT_CMD : {
      void (*callback_put) (void *) = (void (*)(void *))ioctx->private1;
      void *args_put = (void *)ioctx->private2;
      if (callback_put != NULL) {
        callback_put((void *)args_put);
      }
      if(ioctx->key) free(ioctx->key);
      if(ioctx->value) free(ioctx->value);
      break;
    }
    case IOCB_ASYNC_GET_CMD : {
      void (*callback_get) (void *) = (void (*)(void *))ioctx->private1;
      Async_get_context *args_get = (Async_get_context *)ioctx->private2;
      args_get->vbuf = (char*) ioctx->value->value;
      args_get->actual_len = ioctx->value->actual_value_size;
      if (callback_get != NULL) {
        callback_get((void *)args_get->args);
      }
      delete args_get;
      if(ioctx->key) free(ioctx->key);
      if(ioctx->value) free(ioctx->value);
      break;
    }
    // case IOCB_ASYNC_DEL_CMD : {
    //   void (*callback_del) (void *) = (void (*)(void *))ioctx->private1;
    //   void *args_del = (void *)ioctx->private2;
    //   if (callback_del != NULL) {
    //     callback_del((void *)args_del);
    //   }
    //   break;
    // }
    default : {
      printf("aio cmd error \n");
      break;
    }
    }

    return;
  }

  struct iterator_info{
    kvs_iterator_handle iter_handle;
    kvs_iterator_list iter_list;
    int has_iter_finish;
    kvs_iterator_option g_iter_mode;
  };

  bool KVSSD::kv_exist (const Slice *key) {
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    uint8_t result_buf[1];
    const kvs_exist_context exist_ctx = {NULL, NULL};
    kvs_exist_tuples(cont_handle, 1, &kvskey, 1, result_buf, &exist_ctx);
    //printf("[kv_exist] key: %s, existed: %d\n", std::string(key->data(),key->size()).c_str(), (int)result_buf[0]&0x1 == 1);
    return result_buf[0]&0x1 == 1;
  }

  uint32_t KVSSD::kv_get_size(const Slice *key) {
    kvs_tuple_info info;

    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    kvs_result ret = kvs_get_tuple_info(cont_handle, &kvskey, &info);
    if (ret != KVS_SUCCESS) {
        printf("get info tuple failed with err %s\n", kvs_errstr(ret));
        exit(1);
    }
    //printf("[kv_get_size] key: %s, size: %d\n", std::string(key->data(),key->size()).c_str(), info.value_length);
    return info.value_length;
  }

  kvs_result KVSSD::kv_store(const Slice *key, const Slice *val) {
    kvs_store_option option;
    option.st_type = KVS_STORE_POST;
    option.kvs_store_compress = false;

    const kvs_store_context put_ctx = {option, 0, 0};
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    const kvs_value kvsvalue = { (void *)val->data(), val->size(), 0, 0 /*offset */};
    kvs_result ret = kvs_store_tuple(cont_handle, &kvskey, &kvsvalue, &put_ctx);

    if (ret != KVS_SUCCESS) {
        printf("STORE tuple failed with err %s\n", kvs_errstr(ret));
        exit(1);
    }
    stats_.num_store.fetch_add(1, std::memory_order_relaxed);
    //printf("[kv_store] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), val->size());
    return ret;
  }

  kvs_result KVSSD::kv_store_async(Slice *key, Slice *val, void (*callback)(void *), void *args) {
    kvs_store_option option;
    option.st_type = KVS_STORE_POST;
    option.kvs_store_compress = false;

    const kvs_store_context put_ctx = {option, (void *)callback, (void *)args};
    kvs_key *kvskey = (kvs_key*)malloc(sizeof(kvs_key));
    kvskey->key = (void *)key->data();
    kvskey->length = (uint8_t)key->size();
    kvs_value *kvsvalue = (kvs_value*)malloc(sizeof(kvs_value));
    kvsvalue->value = (void *)val->data();
    kvsvalue->length = val->size();
    kvsvalue->actual_value_size = kvsvalue->offset = 0;
    kvs_result ret = kvs_store_tuple_async(cont_handle, kvskey, kvsvalue, &put_ctx, on_io_complete);
    
    if (ret != KVS_SUCCESS) {
        printf("kv_store_async error %s\n", kvs_errstr(ret));
        exit(1);
    }
    stats_.num_store.fetch_add(1, std::memory_order_relaxed);
    return ret;
  }
  // (not support in device)
  // kvs_result KVSSD::kv_append(const Slice *key, const Slice *val) {
  //   kvs_store_option option;
  //   option.st_type = KVS_STORE_APPEND;
  //   option.kvs_store_compress = false;

  //   const kvs_store_context put_ctx = {option, 0, 0};
  //   const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
  //   const kvs_value kvsvalue = { (void *)val->data(), val->size(), 0, 0 /*offset */};
  //   kvs_result ret = kvs_store_tuple(cont_handle, &kvskey, &kvsvalue, &put_ctx);

  //   if (ret != KVS_SUCCESS) {
  //       printf("APPEND tuple failed with err %s\n", kvs_errstr(ret));
  //       exit(1);
  //   }
  //   //printf("[kv_append] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), val->size());
  //   return ret;
  // }

  // inplement append using kv_store and kv_get
  kvs_result KVSSD::kv_append(const Slice *key, const Slice *val) {
    // get old KV
    char *vbuf; int vlen;
    kvs_result ret;
    {
      vbuf = (char *) malloc(INIT_GET_BUFF);
      const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
      kvs_value kvsvalue = { vbuf, INIT_GET_BUFF , 0, 0 /*offset */}; //prepare initial buffer
      kvs_retrieve_option option;
      memset(&option, 0, sizeof(kvs_retrieve_option));
      option.kvs_retrieve_decompress = false;
      option.kvs_retrieve_delete = false;
      const kvs_retrieve_context ret_ctx = {option, 0, 0};
      ret = kvs_retrieve_tuple(cont_handle, &kvskey, &kvsvalue, &ret_ctx);
      if(ret != KVS_ERR_KEY_NOT_EXIST) {
        vlen = kvsvalue.actual_value_size;
        if (INIT_GET_BUFF < vlen) {
          // implement own aligned_realloc
          char *realloc_vbuf = (char *) malloc(vlen + 4 - (vlen%4));
          memcpy(realloc_vbuf, vbuf, INIT_GET_BUFF);
          free(vbuf); vbuf = realloc_vbuf;
          kvsvalue.value = vbuf;
          kvsvalue.length = vlen + 4 - (vlen%4);
          kvsvalue.offset = INIT_GET_BUFF; // skip the first IO buffer (not support, actually read whole value)
          ret = kvs_retrieve_tuple(cont_handle, &kvskey, &kvsvalue, &ret_ctx);
        }
      }

    }
    

    kvs_store_option option;
    option.st_type = KVS_STORE_POST;
    option.kvs_store_compress = false;

    const kvs_store_context put_ctx = {option, 0, 0};
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    kvs_value kvsvalue;
    if (ret == KVS_SUCCESS) { // key exist, append
      vbuf = (char *)realloc(vbuf, vlen+val->size());
      memcpy(vbuf+vlen, val->data(), val->size());
      kvsvalue = { vbuf, vlen+val->size(), 0, 0 /*offset */};
    }
    else { // key not exist, store
      kvsvalue = { (void *)val->data(), val->size(), 0, 0 /*offset */};
    }
    
    ret = kvs_store_tuple(cont_handle, &kvskey, &kvsvalue, &put_ctx);

    if (ret != KVS_SUCCESS) {
        printf("APPEND tuple failed with err %s\n", kvs_errstr(ret));
        exit(1);
    }
    free(vbuf); // release buffer from kv_get
    stats_.num_append.fetch_add(1, std::memory_order_relaxed);
    //printf("[kv_append] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), val->size());
    return ret;
  }


  typedef struct {
    KVSSD *kvd;
    Slice *key;
    Slice *val;
    char *vbuf;
    uint32_t vbuf_size;
    void (*cb) (void *) ;
    void *args;
  } Async_append_context;

  typedef struct {
    char *vbuf;
    void (*cb) (void *) ;
    void *args;
    Async_append_context *append_ctx;
  } Async_append_cleanup;

  static void kv_append_cleanup (void *args) {
    Async_append_cleanup *cleanup = (Async_append_cleanup *)args;
    free(cleanup->vbuf);
    if (cleanup->cb != NULL)
      cleanup->cb(cleanup->args);

    free ((void *)cleanup->append_ctx->key->data());
    free ((void *)cleanup->append_ctx->val->data());
    delete cleanup->append_ctx->key;
    delete cleanup->append_ctx->val;
    delete cleanup->append_ctx;
    delete cleanup;
  }

  static void kv_append_async_callback(void *args) {
    // store new value
    Async_append_context *append_ctx = (Async_append_context *)args;
    
    // append value
    append_ctx->vbuf = (char *)realloc(append_ctx->vbuf, append_ctx->vbuf_size+append_ctx->val->size());  
    memcpy(append_ctx->vbuf+append_ctx->vbuf_size, append_ctx->val->data(), append_ctx->val->size());
    Slice new_val (append_ctx->vbuf, append_ctx->val->size() + append_ctx->vbuf_size);

    Async_append_cleanup *cleanup = new Async_append_cleanup;
    cleanup->vbuf = append_ctx->vbuf;
    cleanup->cb = append_ctx->cb;
    cleanup->args = append_ctx->args;
    cleanup->append_ctx = append_ctx;
    append_ctx->kvd->kv_store_async(append_ctx->key, &new_val, 
                              kv_append_cleanup, cleanup);
    
  }

  // implement async kv append using kv_get_async and kv_store_async
  kvs_result KVSSD::kv_append_async(const Slice *key, const Slice *val, void (*callback)(void *), void *args) {
    // get old KV
    Async_append_context *io_ctx = new Async_append_context;
    io_ctx->kvd = this;
    io_ctx->key = (Slice *)key;
    io_ctx->val = (Slice *)val;
    io_ctx->vbuf = NULL;
    io_ctx->vbuf_size = 0;
    io_ctx->cb = callback;
    io_ctx->args = args;

    Async_get_context *get_ctx = new Async_get_context(io_ctx->vbuf, io_ctx->vbuf_size, io_ctx);

    return kv_get_async(key, kv_append_async_callback, get_ctx);

    stats_.num_append.fetch_add(1, std::memory_order_relaxed);
  }

  kvs_result KVSSD::kv_get(const Slice *key, char*& vbuf, int& vlen) {
    vbuf = (char *) malloc(INIT_GET_BUFF);
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
    kvs_value kvsvalue = { vbuf, INIT_GET_BUFF , 0, 0 /*offset */}; //prepare initial buffer
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;
    const kvs_retrieve_context ret_ctx = {option, 0, 0};
    kvs_result ret = kvs_retrieve_tuple(cont_handle, &kvskey, &kvsvalue, &ret_ctx);
    if(ret == KVS_ERR_KEY_NOT_EXIST) {
      return ret;
    }
    //if (ret == KVS_ERR_BUFFER_SMALL) { // do anther IO KVS_ERR_BUFFER_SMALL not working
    vlen = kvsvalue.actual_value_size;
    if (INIT_GET_BUFF < vlen) {
      // implement own aligned_realloc
      char *realloc_vbuf = (char *) malloc(vlen + 4 - (vlen%4));
      memcpy(realloc_vbuf, vbuf, INIT_GET_BUFF);
      free(vbuf); vbuf = realloc_vbuf;
      kvsvalue.value = vbuf;
      kvsvalue.length = vlen + 4 - (vlen%4);
      kvsvalue.offset = INIT_GET_BUFF; // skip the first IO buffer (not support, actually read whole value)
      ret = kvs_retrieve_tuple(cont_handle, &kvskey, &kvsvalue, &ret_ctx);

      stats_.num_retrieve.fetch_add(1, std::memory_order_relaxed);
      
    }
    stats_.num_retrieve.fetch_add(1, std::memory_order_relaxed);
    //printf("[kv_get] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), vlen);
    return ret;
  }
  // ***** limitations *****
  // currently consider async get buffer size is large enough
  // in other words, async get can retrieve the whole value with 1 I/O.
  kvs_result KVSSD::kv_get_async(const Slice *key, void (*callback)(void *), void *args) {
    char *vbuf = (char *) malloc(INIT_GET_BUFF);
    kvs_key *kvskey = (kvs_key*)malloc(sizeof(kvs_key));
    kvskey->key = (void *) key->data();
    kvskey->length = key->size();
    kvs_value *kvsvalue = (kvs_value*)malloc(sizeof(kvs_value));
    kvsvalue->value = vbuf;
    kvsvalue->length = INIT_GET_BUFF;
    kvsvalue->actual_value_size = kvsvalue->offset = 0;
  
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;
    const kvs_retrieve_context ret_ctx = {option, (void *)callback, (void*)args};
    kvs_result ret = kvs_retrieve_tuple_async(cont_handle, kvskey, kvsvalue, &ret_ctx, on_io_complete);
    if(ret != KVS_SUCCESS) {
      printf("kv_get_async error %d\n", ret);
      exit(1);
    }
    stats_.num_retrieve.fetch_add(1, std::memory_order_relaxed);
    return KVS_SUCCESS;
  }

  // offset must be 64byte aligned (not support)
  kvs_result KVSSD::kv_pget(const Slice *key, char*& vbuf, int count, int offset) {
    vbuf = (char *) malloc(count+64);
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
    kvs_value kvsvalue = { vbuf, count , 0, offset /*offset */}; 
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;
    const kvs_retrieve_context ret_ctx = {option, 0, 0};
    kvs_result ret = kvs_retrieve_tuple(cont_handle, &kvskey, &kvsvalue, &ret_ctx);
    if(ret != KVS_SUCCESS) {
      printf("position get tuple failed with error %s\n", kvs_errstr(ret));
      exit(1);
    }
    //printf("[kv_pget] key: %s, count: %d, offset: %d\n",std::string(key->data(),key->size()).c_str(), count, offset);
    return ret;
  }

  kvs_result KVSSD::kv_delete(const Slice *key) {
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
    const kvs_delete_context del_ctx = { {false}, 0, 0};
    kvs_result ret = kvs_delete_tuple(cont_handle, &kvskey, &del_ctx);

    if(ret != KVS_SUCCESS) {
        printf("delete tuple failed with error %s\n", kvs_errstr(ret));
        exit(1);
    }
    stats_.num_delete.fetch_add(1, std::memory_order_relaxed);
    //printf("[kv_delete] key: %s\n",std::string(key->data(),key->size()).c_str());
    return ret;
  }

  kvs_result KVSSD::kv_scan_keys(std::vector<std::string>& keys) {
    struct iterator_info *iter_info = (struct iterator_info *)malloc(sizeof(struct iterator_info));
    iter_info->g_iter_mode.iter_type = KVS_ITERATOR_KEY;
    
    int ret;
    //printf("start scan keys\n");
    /* Open iterator */

    kvs_iterator_context iter_ctx_open;
    iter_ctx_open.option = iter_info->g_iter_mode;
    iter_ctx_open.bitmask = 0x00000000;

    iter_ctx_open.bit_pattern = 0x00000000;
    iter_ctx_open.private1 = NULL;
    iter_ctx_open.private2 = NULL;
    
    ret = kvs_open_iterator(cont_handle, &iter_ctx_open, &iter_info->iter_handle);
    if(ret != KVS_SUCCESS) {
      printf("iterator open fails with error 0x%x - %s\n", ret, kvs_errstr(ret));
      free(iter_info);
      exit(1);
    }
      
    /* Do iteration */
    iter_info->iter_list.size = ITER_BUFF;
    uint8_t *buffer;
    buffer =(uint8_t*) kvs_malloc(ITER_BUFF, 4096);
    iter_info->iter_list.it_list = (uint8_t*) buffer;

    kvs_iterator_context iter_ctx_next;
    iter_ctx_next.option = iter_info->g_iter_mode;
    iter_ctx_next.private1 = iter_info;
    iter_ctx_next.private2 = NULL;

    while(1) {
      iter_info->iter_list.size = ITER_BUFF;
      memset(iter_info->iter_list.it_list, 0, ITER_BUFF);
      ret = kvs_iterator_next(cont_handle, iter_info->iter_handle, &iter_info->iter_list, &iter_ctx_next);
      if(ret != KVS_SUCCESS) {
        printf("iterator next fails with error 0x%x - %s\n", ret, kvs_errstr(ret));
        free(iter_info);
        exit(1);
      }
          
      uint8_t *it_buffer = (uint8_t *) iter_info->iter_list.it_list;
      uint32_t key_size = 0;
      
      for(int i = 0;i < iter_info->iter_list.num_entries; i++) {
        // get key size
        key_size = *((unsigned int*)it_buffer);
        it_buffer += sizeof(unsigned int);

        // add key
        keys.push_back(std::string((char *)it_buffer, key_size));
        it_buffer += key_size;
      }
          
      if(iter_info->iter_list.end) {
        break;
      } 
    }

    /* Close iterator */
    kvs_iterator_context iter_ctx_close;
    iter_ctx_close.private1 = NULL;
    iter_ctx_close.private2 = NULL;

    ret = kvs_close_iterator(cont_handle, iter_info->iter_handle, &iter_ctx_close);
    if(ret != KVS_SUCCESS) {
      printf("Failed to close iterator\n");
      exit(1);
    }
    
    if(buffer) kvs_free(buffer);
    if(iter_info) free(iter_info);
    return KVS_SUCCESS;
  }
}
