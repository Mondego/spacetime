#ifndef DATAFRAME_CORE_ASYNC_UTILS_H
#define DATAFRAME_CORE_ASYNC_UTILS_H

#include <asio.hpp>
#include <vector>

namespace async_utils {

    constexpr unsigned char map_header = 0xa0; // major type 5, size field empty

    constexpr unsigned char unsigned_int_header = 0x00; // major type 0, size field empty

    bool little_endianess(int num = 1) noexcept;

    const bool is_little_endian = little_endianess();

    unsigned int unpack_unsigned_int(std::array<unsigned char, 4> & data);

    void pack_unsigned_int(std::array<unsigned char, 4> & data, unsigned int value);

    namespace cbor_const {
        constexpr char cbor_char_header = 0x61; // major type 3, length 1
        constexpr char cbor_false = 0xF4; // major type 3, 20
        constexpr char cbor_true = 0xF5; // major type 3, 21

    }

    template <unsigned int count>
    constexpr unsigned char cbor_map_header() {
        static_assert(count < 24, "cannot represent map header with single byte when length of map is greater than 23");
        return  map_header | count;
    }

    template <unsigned int val>
    constexpr unsigned char cbor_small_unsigned_int() {
        static_assert(val < 24, "cannot represent map header with single byte when length of map is greater than 23");
        return unsigned_int_header | val;
    }

    class timeout_error : public std::runtime_error{
        using std::runtime_error::runtime_error;
    };

}

#endif //DATAFRAME_CORE_ASYNC_UTILS_H
