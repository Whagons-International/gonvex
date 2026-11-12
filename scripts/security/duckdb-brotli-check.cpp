#include <brotli/decode.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>

using namespace duckdb_brotli;

int main(int argc, char **argv) {
  if (argc < 2) return 2;
  // Official Node regression vector, also used by DuckDB PR #27791.
  const unsigned char invalid[] = {0x20,0x00,0x10,0x41,0x41,0x41,0xfc,0xff,0xff,0x07,0x00,0x22,0x28,0xf0,0x0b,0x60,0xf7,0xf4,0x1f,0x00,0x00,0x00};
  const unsigned char valid[] = {0x1b,0x1e,0x00,0xf8,0x45,0x4f,0x96,0xea,0x6b,0x93,0x0d,0x23,0xfd,0x1b,0x93,0x28,0x54,0x22,0x50,0x6d,0xf0,0xc5,0xd9,0x78,0x8b,0x87,0x7d,0x82,0xac,0x50,0x8b};
  const unsigned char malformed[] = {0xff,0xff,0xff,0xff};
  bool control = strcmp(argv[1], "valid") == 0;
  bool badHeader = strcmp(argv[1], "malformed") == 0;
  bool chunked = argc > 2 && strcmp(argv[2], "chunked") == 0;
  const unsigned char *next = control ? valid : badHeader ? malformed : invalid;
  size_t remaining = control ? sizeof(valid) : badHeader ? sizeof(malformed) : sizeof(invalid);
  size_t in = chunked ? 1 : remaining, out, total = 0;
  remaining -= in;
  unsigned char output[65536], *nextOut;
  auto *state = BrotliDecoderCreateInstance(nullptr, nullptr, nullptr);
  if (!state) return 2;
  BrotliDecoderResult result;
  for (int step = 0; step < 2048; ++step) {
    out = control ? sizeof(output) - total : sizeof(output);
    nextOut = control ? output + total : output;
    result = BrotliDecoderDecompressStream(state, &in, &next, &out, &nextOut, &total);
    if (result == BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT) continue;
    if (result == BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT && remaining) {
      in = chunked ? 1 : remaining;
      remaining -= in;
      continue;
    }
    break;
  }
  auto error = BrotliDecoderGetErrorCode(state);
  std::printf("%s%s: result=%d error=%d bytes=%zu\n", argv[1], chunked ? " chunked" : "", result, error, total);
  BrotliDecoderDestroyInstance(state);
  if (control) return result == BROTLI_DECODER_RESULT_SUCCESS && total == 31 && std::memcmp(output, "Whagons runtime decoder control", 31) == 0 ? 0 : 1;
  if (badHeader) return result == BROTLI_DECODER_RESULT_ERROR && error < 0 ? 0 : 1;
  return result == BROTLI_DECODER_RESULT_ERROR && error == std::atoi(argv[1]) ? 0 : 1;
}
