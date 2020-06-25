#include <connectors/async_utils.h>

unsigned int async_utils::unpack_unsigned_int(
        std::array<unsigned char, 4> & data) {
    if (!is_little_endian)
        return *reinterpret_cast<unsigned int *>(data.data());
    unsigned int value = *reinterpret_cast<unsigned int *>(data.data());
    std::swap(*reinterpret_cast<unsigned char *>(&value),
              *(reinterpret_cast<unsigned char *>(&value) + 3));
    std::swap(*(reinterpret_cast<unsigned char *>(&value) + 1),
              *(reinterpret_cast<unsigned char *>(&value) + 2));
    return value;
}

void async_utils::pack_unsigned_int(
        std::array<unsigned char, 4> & data, unsigned int value) {
    if (!is_little_endian) {
        memcpy(data.data(), &value, 4);
        return;
    }
    std::swap(*reinterpret_cast<unsigned char *>(&value),
              *(reinterpret_cast<unsigned char *>(&value) + 3));
    std::swap(*(reinterpret_cast<unsigned char *>(&value) + 1),
              *(reinterpret_cast<unsigned char *>(&value) + 2));
    memcpy(data.data(), &value, 4);
}

bool async_utils::little_endianess(int num) noexcept {
    return *reinterpret_cast<char*>(&num) == 1;
}