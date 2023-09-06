#include "memory.h"

#include <cstring>
#include <thread>
#include <vector>

#include <chrono>
#include <iostream>
#include <syncstream>

#include <sstream>

using namespace std::chrono_literals;

namespace unidist {
  uint8_t *pointer_logical_and(const uint8_t *address, uintptr_t bits) {
    uintptr_t value = reinterpret_cast<uintptr_t>(address);
    return reinterpret_cast<uint8_t *>(value & bits);
  }

  uint64_t timeSinceEpochMillisec() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  }

  void my_memcpy( void* dest, const void* src, std::size_t count ) {
    auto start = timeSinceEpochMillisec();

    std::thread::id this_id = std::this_thread::get_id();
 
    // std::this_thread::sleep_for(500ms);
    std::memcpy(dest, src, count);
    auto end = timeSinceEpochMillisec();

    std::ostringstream ss;
    ss << "I am thread [" << this_id << "] from " << start << " to " << end << '\n';
    std::cout << ss.str();
  }

  void parallel_memcopy(uint8_t *dst,
                        const uint8_t *src,
                        int64_t nbytes,
                        uintptr_t block_size,
                        int num_threads) {
    auto start = timeSinceEpochMillisec();
    std::vector<std::thread> threadpool(num_threads);
    uint8_t *left = pointer_logical_and(src + block_size - 1, ~(block_size - 1));
    uint8_t *right = pointer_logical_and(src + nbytes, ~(block_size - 1));
    int64_t num_blocks = (right - left) / block_size;

    // Update right address
    right = right - (num_blocks % num_threads) * block_size;

    // Now we divide these blocks between available threads. The remainder is
    // handled on the main thread.
    int64_t chunk_size = (right - left) / num_threads;
    int64_t prefix = left - src;
    int64_t suffix = src + nbytes - right;
    // Now the data layout is | prefix | k * num_threads * block_size | suffix |.
    // We have chunk_size = k * block_size, therefore the data layout is
    // | prefix | num_threads * chunk_size | suffix |.
    // Each thread gets a "chunk" of k blocks.

    // Start all threads first and handle leftovers while threads run.
    for (int i = 0; i < num_threads; i++) {
      threadpool[i] = std::thread(
          memcpy, dst + prefix + i * chunk_size, left + i * chunk_size, chunk_size);
    }

    std::memcpy(dst, src, prefix);
    std::memcpy(dst + prefix + num_threads * chunk_size, right, suffix);

    for (auto &t : threadpool) {
      if (t.joinable()) {
        t.join();
      }
    }
    auto end = timeSinceEpochMillisec();
    // std::ostringstream ss;
    // ss << "I am parallel_memcopy from " << start << " to " << end << '\n';
    // std::cout << ss.str();
  }
}