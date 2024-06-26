// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/hex.h>

const char * const hex_digit_to_char_uppercase_table = "0123456789ABCDEF";
const char * const hex_digit_to_char_lowercase_table = "0123456789abcdef";

const char * const hex_byte_to_char_uppercase_table = "000102030405060708090A0B0C0D0E0F"
                                                      "101112131415161718191A1B1C1D1E1F"
                                                      "202122232425262728292A2B2C2D2E2F"
                                                      "303132333435363738393A3B3C3D3E3F"
                                                      "404142434445464748494A4B4C4D4E4F"
                                                      "505152535455565758595A5B5C5D5E5F"
                                                      "606162636465666768696A6B6C6D6E6F"
                                                      "707172737475767778797A7B7C7D7E7F"
                                                      "808182838485868788898A8B8C8D8E8F"
                                                      "909192939495969798999A9B9C9D9E9F"
                                                      "A0A1A2A3A4A5A6A7A8A9AAABACADAEAF"
                                                      "B0B1B2B3B4B5B6B7B8B9BABBBCBDBEBF"
                                                      "C0C1C2C3C4C5C6C7C8C9CACBCCCDCECF"
                                                      "D0D1D2D3D4D5D6D7D8D9DADBDCDDDEDF"
                                                      "E0E1E2E3E4E5E6E7E8E9EAEBECEDEEEF"
                                                      "F0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF";

const char * const hex_byte_to_char_lowercase_table = "000102030405060708090a0b0c0d0e0f"
                                                      "101112131415161718191a1b1c1d1e1f"
                                                      "202122232425262728292a2b2c2d2e2f"
                                                      "303132333435363738393a3b3c3d3e3f"
                                                      "404142434445464748494a4b4c4d4e4f"
                                                      "505152535455565758595a5b5c5d5e5f"
                                                      "606162636465666768696a6b6c6d6e6f"
                                                      "707172737475767778797a7b7c7d7e7f"
                                                      "808182838485868788898a8b8c8d8e8f"
                                                      "909192939495969798999a9b9c9d9e9f"
                                                      "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
                                                      "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
                                                      "c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
                                                      "d0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
                                                      "e0e1e2e3e4e5e6e7e8e9eaebecedeeef"
                                                      "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

const char * const hex_char_to_digit_table = "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\xff\xff\xff\xff\xff\xff" //0-9
                                             "\xff\x0a\x0b\x0c\x0d\x0e\x0f\xff\xff\xff\xff\xff\xff\xff\xff\xff" //A-Z
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\xff\x0a\x0b\x0c\x0d\x0e\x0f\xff\xff\xff\xff\xff\xff\xff\xff\xff" //a-z
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                             "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";

const char * const bin_byte_to_char_table = "0000000000000001000000100000001100000100000001010000011000000111"
                                            "0000100000001001000010100000101100001100000011010000111000001111"
                                            "0001000000010001000100100001001100010100000101010001011000010111"
                                            "0001100000011001000110100001101100011100000111010001111000011111"
                                            "0010000000100001001000100010001100100100001001010010011000100111"
                                            "0010100000101001001010100010101100101100001011010010111000101111"
                                            "0011000000110001001100100011001100110100001101010011011000110111"
                                            "0011100000111001001110100011101100111100001111010011111000111111"
                                            "0100000001000001010000100100001101000100010001010100011001000111"
                                            "0100100001001001010010100100101101001100010011010100111001001111"
                                            "0101000001010001010100100101001101010100010101010101011001010111"
                                            "0101100001011001010110100101101101011100010111010101111001011111"
                                            "0110000001100001011000100110001101100100011001010110011001100111"
                                            "0110100001101001011010100110101101101100011011010110111001101111"
                                            "0111000001110001011100100111001101110100011101010111011001110111"
                                            "0111100001111001011110100111101101111100011111010111111001111111"
                                            "1000000010000001100000101000001110000100100001011000011010000111"
                                            "1000100010001001100010101000101110001100100011011000111010001111"
                                            "1001000010010001100100101001001110010100100101011001011010010111"
                                            "1001100010011001100110101001101110011100100111011001111010011111"
                                            "1010000010100001101000101010001110100100101001011010011010100111"
                                            "1010100010101001101010101010101110101100101011011010111010101111"
                                            "1011000010110001101100101011001110110100101101011011011010110111"
                                            "1011100010111001101110101011101110111100101111011011111010111111"
                                            "1100000011000001110000101100001111000100110001011100011011000111"
                                            "1100100011001001110010101100101111001100110011011100111011001111"
                                            "1101000011010001110100101101001111010100110101011101011011010111"
                                            "1101100011011001110110101101101111011100110111011101111011011111"
                                            "1110000011100001111000101110001111100100111001011110011011100111"
                                            "1110100011101001111010101110101111101100111011011110111011101111"
                                            "1111000011110001111100101111001111110100111101011111011011110111"
                                            "1111100011111001111110101111101111111100111111011111111011111111";

const size_t bin_byte_no_zero_prefix_len[256]
    = {1, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6,
       6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
       7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
       7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
       8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
       8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
       8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
