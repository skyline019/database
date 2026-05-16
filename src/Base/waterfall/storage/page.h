////
// @file page.h
// @brief
// 页面
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <stddef.h>
#include <waterfall/schema/scn.h>
#include <waterfall/utils/slice.h>
#include "error.h"
#include "tuple.h"

namespace wf::storage {

using page_id_t = unsigned int;       // 页面ID
using page_offset_t = unsigned short; // 页面偏移量

static const size_t kPageSize = 64 * 1024; // 页面大小

static const size_t kPageMagic = 0x6C6C616672746177; // watrfall，小序
static const size_t kPageChecksumOffset = 16;        // 校验值偏移量

// clang-format off
static const unsigned short kPageTypeCluster = 0b0000000010000000; // clustered数据页面
static const unsigned short kPageTypeHeap =    0b0000000001000000; // heap数据页面
// clang-format on

// 页面头部，32B
class page_header_t
{
  private:
    size_t magic;               // 8B, 页面魔数
    schema::scn_t lsn;          // 8B, 页面LSN
    unsigned int crc32;         // 4B, 页面CRC32校验值
    page_id_t next_page;        // 4B, 下一页偏移量
    page_offset_t free_space;   // 2B, 页面空闲空间偏移量
    unsigned short num_records; // 2B, 页面记录数
    unsigned short free_size;   // 2B, 页面空闲大小
    unsigned short type;        // 2B, 页面类型

  public:
    page_header_t();

    schema::scn_t get_lsn() const { return lsn; }
    void set_lsn(schema::scn_t l) { this->lsn = l; }

    unsigned int get_crc32() const { return crc32; }
    void set_crc32(unsigned int c) { crc32 = c; }

    page_id_t get_next_page() const { return next_page; }
    void set_next_page(page_id_t p) { next_page = p; }

    page_offset_t get_free_space() const { return free_space; }
    void set_free_space(page_offset_t f) { free_space = f; }

    unsigned short get_num_records() const { return num_records; }
    void set_num_records(unsigned short n) { num_records = n; }

    unsigned short get_free_size() const { return free_size; }
    void set_free_size(unsigned short f) { free_size = f; }

    unsigned short get_type() const { return type; }
    void set_type(unsigned short t) { type = t; }

    bool is_clustered_page() const { return (type & kPageTypeCluster) != 0; }
    void set_clustered_page() { type |= kPageTypeCluster; }
    bool is_heap_page() const { return (type & kPageTypeHeap) != 0; }
    void set_heap_page() { type |= kPageTypeHeap; }
};

// 页面尾部slots
class page_slots
{
  private:
    unsigned char *page_; // 指向页面

  public:
    page_slots(unsigned char *page)
        : page_(page)
    {}

    // slots起始位置
    page_offset_t *slots_begin();
    // 插入slot
    void insert_slot(page_offset_t record);
    // 查找slots
    page_offset_t find_slot(utils::char_slice &key);

  private:
    // 排序slots
    void sort_slots();
};

// 页面基类
class page_base
{
  protected:
    unsigned char *page_; // 指向页面

  public:
    page_base(unsigned char *page)
        : page_(page)
    {}

    unsigned int compute_checksum() const;
};

// 数据页面
class data_page : public page_base
{
  public:
    data_page(unsigned char *page)
        : page_base(page)
    {}

    error_info insert_record(unsigned short header, const logical_tuple &tuple);
    void remove_record(utils::char_slice &key);
    error_info update_record(const logical_tuple &tuple);
};

// heap页面
class heap_page : public page_base
{
  public:
    heap_page(unsigned char *page)
        : page_base(page)
    {}
};

// superblock页面
class catalog_page : public page_base
{
  public:
    catalog_page(unsigned char *page)
        : page_base(page)
    {}
};

} // namespace wf::storage