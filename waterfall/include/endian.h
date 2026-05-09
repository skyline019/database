#pragma once

#if defined(_WIN32)
#include <stdint.h>
#include <stdlib.h>

#ifndef __BYTE_ORDER
#define __LITTLE_ENDIAN 1234
#define __BIG_ENDIAN 4321
#define __BYTE_ORDER __LITTLE_ENDIAN
#endif

#ifndef htole16
#define htole16(x) ((uint16_t)(x))
#endif
#ifndef le16toh
#define le16toh(x) ((uint16_t)(x))
#endif
#ifndef htole32
#define htole32(x) ((uint32_t)(x))
#endif
#ifndef le32toh
#define le32toh(x) ((uint32_t)(x))
#endif
#ifndef htole64
#define htole64(x) ((uint64_t)(x))
#endif
#ifndef le64toh
#define le64toh(x) ((uint64_t)(x))
#endif

#ifndef htobe16
#define htobe16(x) _byteswap_ushort((uint16_t)(x))
#endif
#ifndef be16toh
#define be16toh(x) _byteswap_ushort((uint16_t)(x))
#endif
#ifndef htobe32
#define htobe32(x) _byteswap_ulong((uint32_t)(x))
#endif
#ifndef be32toh
#define be32toh(x) _byteswap_ulong((uint32_t)(x))
#endif
#ifndef htobe64
#define htobe64(x) _byteswap_uint64((uint64_t)(x))
#endif
#ifndef be64toh
#define be64toh(x) _byteswap_uint64((uint64_t)(x))
#endif

#endif  // _WIN32
