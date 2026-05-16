////
// @file page.cc
// @brief
// 页面实现
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <string.h>
#include <crc32c/crc32c.h>
#include "page.h"

namespace wf::storage {

// lsn，无法初始化填写，lsn是日志序列号
// crc32，无法初始化填写，crc32是校验值
// next_page，无法初始化填写，next_page是下一页偏移量
page_header_t::page_header_t()
    : magic(kPageMagic)
    , lsn(0)
    , crc32(0)
    , next_page(0)
    , free_space(sizeof(page_header_t))
    , num_records(0)
    , free_size(kPageSize - sizeof(page_header_t))
    , type(0)
{}

page_offset_t *page_slots::slots_begin()
{
    page_header_t *header = (page_header_t *) page_;
    return (page_offset_t *) page_ + kPageSize - header->get_num_records();
}

unsigned int page_base::compute_checksum() const
{
    // 第1部分
    unsigned int checksum = crc32c::Extend(0, page_, kPageChecksumOffset);
    // 第2部分
    page_offset_t start = kPageChecksumOffset + sizeof(unsigned int);
    return crc32c::Extend(checksum, page_ + start, kPageSize - start);
}

} // namespace wf::storage