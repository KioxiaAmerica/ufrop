//  Copyright (c) 2024, Kioxia corporation.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <cinttypes>
#include <cstdio>

#include <stdexcept>
#include <iostream>
#include <array>
#include <set>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <shared_mutex>
#include <algorithm>
#include <thread>
#include <regex>
#include <bitset>
#include <ranges>
#include <string_view>

#include <string>
#include <cassert>
#include <iostream>
#include <cstring>
#include <unordered_map>
#include <vector>
#include <limits>
#include <mutex>
#include <thread>

#include <cerrno>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <nvme/ioctl.h>
#include <nvme/api-types.h>
#include <linux/byteorder/little_endian.h>

//#include <boost/stacktrace.hpp>
#include <boost/lockfree/queue.hpp>

#define ASSERT_LOCK_IS_HELD(lock) assert(!(lock).try_lock());

template<typename T>
static inline T __round_mask(const T& /*x*/, const T& y) {
        return y - 1;
}

template<typename T>
static inline T round_up(const T& x, const T& y) {
        const T& tmp = ((x - 1) | __round_mask(x, y)) + 1;
        assert((tmp % y) == 0);
        return tmp;
}

template<typename T>
static inline T round_down(const T& x, const T& y) {
        const T& tmp = x & ~__round_mask(x, y);
        assert((tmp % y) == 0);
        return tmp;
}

static inline std::vector<std::string>
split_by(const std::string& str, char del) {
        std::vector<std::string> ret;
        size_t first = 0;
        size_t last = str.find_first_of(del);

        while (first < str.size()) {
                const auto& sub = str.substr(first, last - first);
                if (!sub.empty()) {
                        ret.push_back(sub);
                }
                first = last + 1;
                last = str.find_first_of(del, first);
                if (last == std::string::npos) {
                        last = str.size();
                }
        }
        return ret;
}

class PlacementId {
        uint32_t id_;
public:
        static constexpr uint32_t NONE = 0;
        static constexpr uint32_t METADATA = 1;
        static constexpr uint32_t DATA_SHORT = 2;
        static constexpr uint32_t DATA_MEDIUM = 3;
        static constexpr uint32_t DATA_LONG = 4;
        static constexpr uint32_t DATA_EXTREME = 5;
        static constexpr uint32_t DATA_SEQUENTIAL = 6;
        static constexpr uint32_t DATA_WRITABLE = 7;
        static constexpr uint32_t MAX = 7;
        PlacementId() : id_(NONE) {}
        PlacementId(uint32_t id) : id_(id) {}
        PlacementId(const PlacementId&) = default;
        ~PlacementId() = default;

        uint32_t directive_spec() const {
                if (id_ > MAX) {
                        return MAX;
                }
                return id_;
        }
        constexpr uint32_t directive_type() const {
                return 2; // use fdp directive
        }
        PlacementId map(const std::vector<uint32_t>& m) const {
                return PlacementId(m[id_]);
        }

};

ssize_t Pwrite(int fd, const void* buf, size_t count, uint64_t offset, [[maybe_unused]] PlacementId) {
        auto n = ::pwrite(fd, buf, count, offset);
        if (n < 0 || static_cast<decltype(count)>(n) != count) {
                std::cerr << __FUNCTION__ << " " << std::strerror(errno) << std::endl;
                std::cerr << "return value " << n << " count " << count << " offset " << offset << std::endl;
                throw std::runtime_error(std::strerror(errno));
        }
        return n;
}

ssize_t Pread(int fd, void* buf, size_t count, uint64_t offset) {
        auto n = ::pread(fd, buf, count, offset);
        if (n < 0) {
                std::cerr << __FUNCTION__ << " " << std::strerror(errno) << std::endl;
                throw std::runtime_error(std::strerror(errno));
        }
        if(static_cast<decltype(count)>(n) != count) {
                std::cerr << __FUNCTION__ << " partial read " << count << " actual " << n << std::endl;
        }
        return n;
}

static inline
void DiscardSegment(int /*fd*/, uint64_t /*offset*/, uint64_t /*count*/) {
        // XXX: dicard by ioctl
}

void Fsync(int fd) {
        [[maybe_unused]] int n = fsync(fd);
        assert(n == 0);
}

class spinlock {
private:
        pthread_spinlock_t lock_;

public:
        spinlock() {
                int n = pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
                if (n != 0) {
                        std::string msg(std::string("pthread_spin_init failed: ") + std::strerror(errno));
                        std::cerr << msg << std::endl;
                        throw std::runtime_error(msg);
                }
        }
        ~spinlock() {
                int n = pthread_spin_destroy(&lock_);
                if (n != 0) {
                        std::cerr << "pthread_spin_destroy failed: " << std::strerror(errno) << std::endl;
                }

        }
        spinlock(const spinlock&) = delete;
        spinlock& operator = (const spinlock&) = delete;

        void lock() {
                int n = pthread_spin_lock(&lock_);
                if (n != 0) {
                        std::string msg(std::string("pthread_spin_lock failed: ") + std::strerror(errno));
                        std::cerr << msg << std::endl;
                        throw std::runtime_error(msg);
                }
        }
        void unlock() {
                int n = pthread_spin_unlock(&lock_);
                if (n != 0) {
                        std::string msg(std::string("pthread_spin_lock failed: ") + std::strerror(errno));
                        std::cerr << msg << std::endl;
                        throw std::runtime_error(msg);
                }
        }
        bool try_lock() noexcept {
                int n = pthread_spin_trylock(&lock_);
                bool acquire = (n == 0); // zero means successfull lock aquisition
                if (!acquire) {
                        std::this_thread::yield();
                }
                return acquire;
        }
};

struct RingResult {
#ifndef NDEBUG
        void* buf_;
        nvme_io_opcode op_;
        size_t count_;
        off_t offset_;
        std::thread::id id_;
#endif
        int errno_;
        std::atomic<bool> completed_;
        RingResult(
#ifndef NDEBUG
               void* buf, nvme_io_opcode op, size_t count, off_t offset, std::thread::id id
#endif
               )
                :
#ifndef NDEBUG
                buf_(buf), op_(op), count_(count), offset_(offset), id_(id),
#endif
                errno_(0), completed_(false) {}
};


struct RingRequest {
        io_uring_sqe sqe;
        nvme_uring_cmd cmd;
        std::atomic<bool> submitted_;
        RingRequest(RingResult* result_, void* buf_, nvme_io_opcode op_, uint32_t nsid_, int fd_, size_t count_, off_t offset_, PlacementId pid, bool use_fdp) : submitted_(false) {
                uint64_t slba = offset_ / 4096;
                uint32_t nlb = count_ / 4096 - 1;
                uint32_t directive_spec = (use_fdp and op_ == nvme_cmd_write) ? pid.directive_spec() : 0;
                uint32_t directive_type = (use_fdp and op_ == nvme_cmd_write) ? pid.directive_type() : 0;
                assert(fd_ >= 0);
                sqe = io_uring_sqe { .opcode = IORING_OP_URING_CMD, .flags = 0,
                                     .fd = fd_, .cmd_op = NVME_URING_CMD_IO,
                                     .user_data = reinterpret_cast<uintptr_t>(result_) };
                
                assert(count_ <= std::numeric_limits<uint32_t>::max());
                if (op_ == nvme_cmd_dsm) {
                        assert(buf_ != nullptr);
                        nvme_dsm_range* r = static_cast<nvme_dsm_range*>(buf_);
                        r->slba = slba;
                        r->nlb = nlb + 1;
                        static constexpr uint32_t NVME_ATTRIBUTE_DEALLOCATE = (1 << 2);
                        cmd = nvme_uring_cmd { .opcode = op_,
                                               .nsid = nsid_,
                                               .addr = reinterpret_cast<uintptr_t>(r),
                                               .data_len = sizeof(struct nvme_dsm_range),
                                               .cdw10 = 0,
                                               .cdw11 = NVME_ATTRIBUTE_DEALLOCATE,
                        };
                        // std::cout << "nvme deallocate offset " << offset_ << " count " << count_ << std::endl;
                }
                else if (op_ == nvme_cmd_flush) {
                        assert(buf_ == nullptr);
                        cmd = nvme_uring_cmd { .opcode = op_,
                                               .nsid = nsid_,
                        };
                }
                else {
                        assert(buf_ != nullptr);
                        cmd = nvme_uring_cmd { .opcode = op_,
                                               .nsid = nsid_,                                
                                               .addr = reinterpret_cast<uintptr_t>(buf_),
                                               .data_len = static_cast<uint32_t>(count_),
                                               .cdw10 = static_cast<uint32_t>(slba & 0xffffffffUL),
                                               .cdw11 = static_cast<uint32_t>(slba >> 32),
                                               .cdw12 = nlb | (directive_type << 20),
                                               .cdw13 = directive_spec << 16,
                        };
                }
                
        }
};

static constexpr size_t FDP_MAX_RUHS = 32;

class RingContext {
        io_uring* ring_;
        int fd_;
        uint32_t nsid_;

        boost::lockfree::queue<RingRequest*>* request_queue_;
        spinlock* submit_mtx_;
        spinlock* complete_mtx_;

        bool use_fdp_;
        bool use_discard_;
        std::vector<uint32_t> pid_map_;

public:
        RingContext() : ring_(nullptr), fd_(-1),request_queue_(nullptr), submit_mtx_(nullptr), complete_mtx_(nullptr) {}
        RingContext(const RingContext&) = default;
        RingContext& operator = (const RingContext&) = default;

        RingContext(unsigned int qd, const std::string& path, bool use_fdp, bool use_discard) : use_fdp_(use_fdp), use_discard_(use_discard) {
                std::cout << __FUNCTION__ << " " << path << " fdp = " << use_fdp << " discard = " << use_discard <<std::endl;

                ring_ = new io_uring;
                int ret = io_uring_queue_init(qd, ring_, IORING_SETUP_SQE128|IORING_SETUP_CQE32);
                if (ret < 0) {
                        std::cout << "io_uriong_queue_init " << std::strerror(-ret) << std::endl;
                        throw std::runtime_error("io_uring_queue_init");
                }
        
                int fd = open(path.c_str(), O_RDWR|O_CLOEXEC);
                if (fd < 0) {
                        std::cout << "open error " << path << ": " << std::strerror(errno) << " " << errno << std::endl;
                        throw std::runtime_error("open " + path);
                }
                fd_ = fd;
                std::cout << "open " << path << " for iouring with fd " << fd << std::endl;

                int namespace_id = ioctl(fd, NVME_IOCTL_ID);
                if (namespace_id < 0) {
                        std::cerr << "ioctl failed: " << std::strerror(errno) << " FILE " << __FILE__ << " LINE " << __LINE__ << std::endl;
                        throw std::runtime_error("ioctl nvme");
                }
                std::cout << "namespace_id " << namespace_id << std::endl;
                nsid_ = namespace_id;

                request_queue_ = new boost::lockfree::queue<RingRequest*>(qd);
                submit_mtx_ = new spinlock;
                complete_mtx_ = new spinlock;

                //init_pid_map();
        }
        ~RingContext() {}

        void close() {
                if (fd_ < 0) {
                        return;
                }
                int ret = ::close(fd_);
                if (ret < 0) {
                        std::cout << "close " << std::strerror(errno) << std::endl;
                }
                assert(ring_ != nullptr);
                io_uring_queue_exit(ring_);
                delete ring_;
                ring_ = nullptr;

                delete request_queue_;
                delete submit_mtx_;
                delete complete_mtx_;
        }

        boost::lockfree::queue<RingRequest*>* get_request_queue() {
                return request_queue_;
        }
        spinlock* get_submit_spinlock() {
                return submit_mtx_;
        }
        
        spinlock* get_complete_spinlock() {
                return complete_mtx_;
        }
        
        io_uring* get_ring() {
                return ring_;
        }
        int get_fd() const {
                return fd_;
        }

        int identify(nvme_identify_cns cns,
                     nvme_csi csi, void *data) const {
                constexpr uint64_t NVME_IDENTIFY_CSI_SHIFT = 24;
                struct nvme_passthru_cmd cmd = {
                        .opcode         = nvme_admin_identify,
                        .nsid           = nsid_,
                        .addr           = (__u64)(uintptr_t)data,
                        .data_len       = NVME_IDENTIFY_DATA_SIZE,
                        .cdw10          = cns,
                        .cdw11          = static_cast<uint32_t>(csi << NVME_IDENTIFY_CSI_SHIFT),
                        .timeout_ms     = NVME_DEFAULT_IOCTL_TIMEOUT,
                };

                return ioctl(fd_, NVME_IOCTL_ADMIN_CMD, &cmd);
        }

        const std::vector<uint32_t>& get_pid_map() const {
                return pid_map_;
        }
        
        void init_pid_map() {
                constexpr uint32_t bytes = 4096;
                static_assert(bytes >= sizeof(nvme_fdp_ruh_status) + FDP_MAX_RUHS * sizeof(nvme_fdp_ruh_status_desc));
                nvme_fdp_ruh_status* ruhs = static_cast<nvme_fdp_ruh_status*>(malloc(bytes));
                if (ruhs == nullptr) {
                        return;
                }

                int ret = reclaim_unit_info(bytes, ruhs);
                if (ret != 0) {
                        //std::cout << "reclaim unit info erro " << std::strerror(errno) << " " << std::strerror(ret) << " " << std::strerror(-ret) << std::endl;
                        printf("reclaim unit info err 0x%x\n", ret);
                        free(ruhs);
                        return;
                }
                size_t nr = le16toh(ruhs->nruhsd);
                std::vector<uint32_t> ret_ruhs(nr);
                std::cout << __FUNCTION__ << " num ruh " << nr << std::endl;

                for (size_t i = 0; i < nr; i++) {
                        ret_ruhs[i] = le16toh(ruhs->ruhss[i].pid);
                        [[maybe_unused]] uint32_t earutr = le32toh(ruhs->ruhss[i].earutr); //Estimated Active Reclaim Unit Time Remaining in sec
                        [[maybe_unused]] uint64_t ruamw =  le64toh(ruhs->ruhss[i].ruamw); // Reclaim Unit Available Media Writes in number of logical blocs
                        std::cout << "ruh[" << i << "] = " << ret_ruhs[i] << " earutr " << earutr << " ruamw "<< ruamw << std::endl;
                }
                free(ruhs);
                pid_map_ = ret_ruhs;
        }
        
        int reclaim_unit_info(uint32_t data_len, void *data) const {
                static constexpr uint8_t nvme_cmd_io_mgmt_recv = 0x12;
                struct nvme_passthru_cmd cmd = {
                        .opcode         = nvme_cmd_io_mgmt_recv,
                        .nsid           = nsid_,
                        .addr           = (__u64)(uintptr_t)data,
                        .data_len       = data_len,
                        .cdw10          = 1,
                        .cdw11          = (data_len >> 2) - 1,
                };

                return ioctl(fd_, NVME_IOCTL_IO_CMD, &cmd);
        }

        int reclaim_unit_update(const std::vector<uint32_t>& pil) const {
                // TP4146a p.51-52
                static constexpr uint8_t nvme_cmd_io_mgmt_send = 0x1d;
                const uint32_t data_len = pil.size() * sizeof(uint32_t);
                struct nvme_passthru_cmd cmd = {
                        .opcode         = nvme_cmd_io_mgmt_send,
                        .nsid           = nsid_,
                        .addr           = (__u64)(uintptr_t)&pil[0],
                        .data_len       = data_len,
                        .cdw10          = 1, // MO: reclaim unit handle update
                        .cdw11          = (data_len >> 2) - 1,
                };

                return ioctl(fd_, NVME_IOCTL_IO_CMD, &cmd);
        }

        int reclaim_unit_update(uint32_t pi) const {
                return reclaim_unit_update({pi});
        }

        uint64_t dev_len() const {
                struct nvme_id_ns ns;
                memset(&ns, 0, sizeof(ns));
                int err = identify(NVME_IDENTIFY_CNS_NS, NVME_CSI_NVM, &ns);
                if (err) {
                        std::cerr << "ioctl identify failed: " << std::strerror(errno) << " FILE " << __FILE__ << " LINE " << __LINE__ << std::endl;
                        throw std::runtime_error("ioctl nvme identify");
                }

                [[maybe_unused]] uint32_t format_idx =  (ns.nlbaf < 16) ? (ns.flbas & 0xf) : (ns.flbas & 0xf) + (((ns.flbas >> 5) & 0x3) << 4);
                [[maybe_unused]] uint32_t lba_size = 1 << ns.lbaf[format_idx].ds;
                assert(lba_size == 4096);
                std::cout << "ns.nlbaf " << static_cast<uint64_t>(ns.nlbaf) << " lba_size " << lba_size << std::endl;
                
                return ns.nsze * 4096UL;
        }
        uint32_t get_nsid() const {
                return nsid_;
        }

        bool use_fdp() const {
                return use_fdp_;
        }

        bool use_discard() const {
                return use_discard_;
        }
};

static inline ssize_t RingDo(RingContext* c, void* buf, size_t count, off_t offset, nvme_io_opcode op, PlacementId pid) {

        boost::lockfree::queue<RingRequest*>* reqs = c->get_request_queue();
        spinlock* submit_mtx = c->get_submit_spinlock();
        spinlock* complete_mtx = c->get_complete_spinlock();
        assert(reqs != nullptr);
        assert(submit_mtx != nullptr);
        assert(complete_mtx != nullptr);
        
        io_uring* ring = c->get_ring();
        assert(ring != nullptr);

#ifdef NDEBUG
        RingResult result;
#else
        RingResult result(buf, op, count, offset, std::this_thread::get_id());
#endif
     
        //[[maybe_unused]] auto& m = c->get_pid_map();

        RingRequest req(&result, buf, op, c->get_nsid(), c->get_fd(), count, offset, pid, c->use_fdp());
        while (!reqs->push(&req)) {} // push may fail

        while (!req.submitted_.load()) {
                std::unique_lock<spinlock> lock(*submit_mtx, std::defer_lock);
                if (!lock.try_lock()) {
                        continue;
                }

                int submit_count = 0;
                RingRequest* req_acc = nullptr;
                while (reqs->pop(req_acc)) {
                        assert(req_acc != nullptr);
                        assert(!req_acc->submitted_.load());
                        req_acc->submitted_.store(true);
                        io_uring_sqe* sqe = io_uring_get_sqe(ring);
                        assert(sqe != nullptr);
                        *sqe = req_acc->sqe;
                        *reinterpret_cast<nvme_uring_cmd*>(sqe->cmd) = req_acc->cmd;
                        submit_count++;
                        req_acc = nullptr;
                }
                if (submit_count == 0) {
                        continue;
                }
                int ret = io_uring_submit(ring);
                if (ret < 0) {
                        std::cout << "io_uriong_submit " << std::strerror(-ret) << std::endl;
                        errno = -ret;
                        return -1;
                }
                assert(submit_count == ret);

        }
        assert(req.submitted_.load());

        while (true) {
                if (result.completed_.load()) {
                        assert(result.buf_ == buf and
                               result.op_ == op and
                               result.count_ == count and
                               result.offset_ == offset and
                               result.id_ == std::this_thread::get_id());
                        int err = result.errno_;
                        if (err > 0) {
                                errno = err;
                                return -1;
                        }
                        else {
                                return count;
                        }
                }
                
                std::unique_lock<spinlock> lock(*complete_mtx, std::defer_lock);
                if (!lock.try_lock()) {
                        continue;
                }
                io_uring_cqe* cqes[64];
                unsigned nr = io_uring_peek_batch_cqe(ring, cqes, 64);
                if (nr == 0) {
                        continue;
                }
                for (size_t i = 0; i < nr; i++) {
                        io_uring_cqe* cqe = cqes[i];
                        auto r = reinterpret_cast<RingResult*>(cqe->user_data);
                        if (cqe->res < 0) {
                                std::cout << "cqe->res " << std::strerror(-cqe->res) << " op " << op << " nsid "<< c->get_nsid()
                                          << " offset " << offset << " count " << count << std::endl;
                                r->errno_ =  -cqe->res;
                        }
                        assert(!r->completed_.load());
                        r->completed_.store(true);
                }
                io_uring_cq_advance(ring, nr);
        }
        __builtin_unreachable();
}

constexpr uint64_t io_size = 1024*128;
static inline ssize_t Pread(RingContext& c, void* buf, size_t count, off_t offset) {
        const uint64_t n = count / io_size;
        const uint64_t rem = count % io_size;

        assert((io_size * n + rem) == count);
        
        assert((offset % 4096) == 0);
        assert((count % 4096) == 0);
        assert((rem % 4096) == 0);
#ifndef NDEBUG
        memset(buf, 0, count);
#endif
        const PlacementId placement_id = PlacementId::NONE;

        ssize_t ret = 0;
        [[maybe_unused]] uint64_t k = 0;
        for (size_t m = 0; m < n; m++) {
                uint64_t off = m * io_size;
                ssize_t r = RingDo(&c, static_cast<void*>(static_cast<char*>(buf) + off), io_size, offset + off, nvme_cmd_read, placement_id);
                if (r < 0) {
                        return r;
                }
                assert(static_cast<uint64_t>(r) == io_size);
                ret += r;
                k++;
        }
        assert(k == n);
        if (rem != 0) {
                ssize_t r = RingDo(&c, static_cast<void*>(static_cast<char*>(buf) + (n * io_size)), rem, offset + (n * io_size), nvme_cmd_read, placement_id);
                if (r < 0) {
                        return r;
                }
                assert(static_cast<uint64_t>(r) == rem);
                ret += r;
        }
        
        assert(static_cast<size_t>(ret) == count);
        return ret;
        // return RingDo(&c, buf, count, offset, nvme_cmd_read, placement_id);
}

static inline ssize_t Pwrite(RingContext& c, const void* buf, size_t count, off_t offset, PlacementId placement_id) {
        const uint64_t n = count / io_size;
        const uint64_t rem = count % io_size;

        assert((io_size * n + rem) == count);
        
        assert((offset % 4096) == 0);
        assert((count % 4096) == 0);
        assert((rem % 4096) == 0);

        ssize_t ret = 0;
        [[maybe_unused]] uint64_t k = 0;
        for (size_t m = 0; m < n; m++) {
                uint64_t off = m * io_size;
                ssize_t r = RingDo(&c, const_cast<void*>(static_cast<const void*>(static_cast<const char*>(buf) + off)), io_size, offset + off, nvme_cmd_write, placement_id);
                if (r < 0) {
                        return r;
                }
                if (static_cast<uint64_t>(r) != io_size) {
                        std::cout << __FUNCTION__ << " ring do ret " << r << " io_size " << io_size << " count " << count << std::endl;
                }
                assert(static_cast<uint64_t>(r) == io_size);
                ret += r;
                k++;
        }
        assert(k == n);
        if (rem != 0) {
                ssize_t r = RingDo(&c, const_cast<void*>(static_cast<const void*>(static_cast<const char*>(buf) + (n*io_size))), rem, offset + (n*io_size), nvme_cmd_write, placement_id);
                if (r < 0) {
                        return r;
                }
                if (static_cast<uint64_t>(r) != rem) {
                        std::cout << __FUNCTION__ << " ring do ret " << r << " rem " << rem << " count " << count << std::endl;
                }
                assert(static_cast<uint64_t>(r) == rem);
                ret += r;
        }
        
        assert(static_cast<size_t>(ret) == count);
        return ret;
}

static inline void DiscardSegment([[maybe_unused]] RingContext& c, [[maybe_unused]] uint64_t offset, [[maybe_unused]] uint64_t count) {
        if (c.use_discard()) {
                nvme_dsm_range range;
                [[maybe_unused]] auto s = RingDo(&c, &range, count, offset, nvme_cmd_dsm, PlacementId::NONE);
                //std::cout << "dsm deallocate offset "  << offset << " count " << count << std::endl;
                //RingDo(RingContext* c, void* buf, size_t count, off_t offset, nvme_io_opcode op, PlacementId pid) {
        }
}

static inline void Fsync([[maybe_unused]] RingContext& c) {
        // [[maybe_unused]] auto s = RingDo(&c, nullptr, 0, 0, nvme_cmd_flush, PlacementId::NONE);
}


#define CHECK_FD(fd) (fd).CHECK()

template<typename T>
class FdpFs {
private:
        enum class SuffixType : uint32_t {
                WAL = 0,
                SST = 1,
                MANIFEST = 2,
                CURRENT = 3,
                IDENTITY = 4,
                OPTION = 5,
                LOG = 6,
                DBTMP = 7,
                OPTION_DBTMP = 8,
                LOG_OLD = 9,
                LOCK = 10,
                NONE = 11,
                GENERAL = 12,
                LDB = 13,
                BLOB= 14,
        };

        struct FileName {
                uint64_t name_;
                SuffixType suffix_;
                //FileName() : name_(0), suffix_(SuffixType::NONE) {}
                FileName(uint64_t name, SuffixType suffix) : name_(name), suffix_(suffix) {}
                FileName(const FileName& rhs) : name_(rhs.name_), suffix_(rhs.suffix_) {}
                FileName(FileName&& rhs) : name_(rhs.name_), suffix_(rhs.suffix_) {}
                FileName& operator = (const FileName& rhs) {
                        name_ = rhs.name_;
                        suffix_ = rhs.suffix_;
                        return *this;
                }
                bool operator == (const FileName& rhs) const {
                        return name_ == rhs.name_ and suffix_ == rhs.suffix_;
                }
                bool operator != (const FileName& rhs) const {
                        return name_ != rhs.name_ or suffix_ != rhs.suffix_;
                }
        };
    
        struct File;

        using DirType = uint32_t;

        struct Path {
                std::vector<DirType> dirname_;
                FileName filename_;
                // Path() {}
                Path(const std::vector<DirType>& dirname, const FileName& filename) : dirname_(dirname), filename_(filename) {}
                bool operator == (const Path& rhs) const {
                        return dirname_ == rhs.dirname_ and filename_ == rhs.filename_;
                }
                bool operator != (const Path& rhs) const {
                        return not(*this == rhs);
                }
        };

        mutable std::vector<std::string> dirs_map_;
public:

        enum class OperationMode {
                ReadOnly, AppendOnly, ReadAppend, WriteOnly, ReadWrite,
        };
        enum class CreateMode {
                CreateIfMissing, ErrorIfMissing,
        };


        class ClassMessageException : public std::exception {
                std::string msg_;
        public:
                ClassMessageException(const std::string& m, const std::string& f) : msg_(m + f) {}
                virtual ~ClassMessageException() {}
                virtual const char* what() const noexcept override {
                        return msg_.c_str();
                }
        };

        class NoSpace: public std::exception {
        public:
                virtual const char* what() const noexcept override {
                        return "There is no space";
                }
        };

        class FileNotFound : public ClassMessageException {
        public:
                FileNotFound(const std::string& f) : ClassMessageException("FileNotFound: ", f) {}
        };

        class DirNotFound : public ClassMessageException {
        public:
                DirNotFound(const std::string& f) : ClassMessageException("DirNotFound: ", f) {}
        };

        class InvalidOperation : public ClassMessageException {
        public:
                InvalidOperation(const std::string& f) : ClassMessageException("InvalidOperation: ", f) {}
        };

        class FrozenFile : public ClassMessageException {
        public:
                FrozenFile(const std::string& f) : ClassMessageException("FrozenFile: ", f) {}
        };

        class FileAlreadyOpen : public ClassMessageException {
        public:
                FileAlreadyOpen(const std::string& f) : ClassMessageException("FileAlreadyOpen: ", f) {}
        };
        class TooManyOpenFiles : public ClassMessageException {
        public:
                TooManyOpenFiles(const std::string& f) : ClassMessageException("TooManyOpenFiles: ", f) {}
        };
        

        class FdpFsFd {
        private:
                T device_;
                uint64_t length_, cur_;
                Path file_;
                OperationMode om_;
                bool closed_;
                uint64_t inode_;
                std::vector<uint64_t> blocks_;
                PlacementId placement_id_;
                friend class FdpFs;
                // only constructable by friend classes
                FdpFsFd(const FdpFs& fs, const File& f, uint64_t offset, OperationMode om, PlacementId placement_id) :
                        device_(fs.device_), length_(f.length_), 
                        cur_(0), file_(f.getPath()), om_(om), closed_(false), inode_(offset), placement_id_(placement_id) {
                        uint64_t direct_block_ = fs.begin_file_offset_ + fs.block_length_ * offset;
                        blocks_.push_back(direct_block_);
                        // std::cout << "FdpFsFd length " << length_ <<  " direct block " << direct_block_ << " indirect_blocks_num " << f.indirect_blocks_num_<< std::endl;
                        for (size_t i = 0; i < f.indirect_blocks_num_; i++) {
                                uint64_t inb = fs.begin_file_offset_ + fs.block_length_ * f.indirect_blocks_[i];
                                blocks_.push_back(inb);
                                std::cout << "indirect block " << f.indirect_blocks_[i] << " byte " << inb << std::endl;
                                assert(!fs.free_blocks.contains(f.indirect_blocks_[i]));
                        }
                        assert((length_ / fs.block_length_) + 1 <= blocks_.size());
                }
                FdpFsFd(const FdpFsFd&) = delete;
                FdpFsFd& operator = (const FdpFsFd&) = delete;
        public:
                FdpFsFd(FdpFsFd&& rhs) : file_(rhs.file_) {
                        device_ = rhs.device_; // copy
                        length_ = rhs.length_;
                        cur_ = rhs.cur_;
                        // file_ = rhs.file_;
                        om_ = rhs.om_;
                        closed_ = rhs.closed_;
                        inode_ = rhs.inode_;
                        blocks_ = rhs.blocks_;
                        placement_id_ = rhs.placement_id_;
                        rhs.closed_ = true;

                }
                void CHECK() const {
                        assert(!closed_);
                        // 12 10 / blocks_
                        // if (cur_ > length_) {
                        //         std::cout << "CHECK cur_ < length_ " << cur_ << " < " << length_ << std::endl;
                        //         assert(cur_ <= length_);
                        // }
                }
                Path getPath() const {
                        return file_;
                }
                std::string getPathStr() const {
                        return path2str(file_);
                }

                bool IsAppendable() const {
                        return (om_ == OperationMode::AppendOnly or om_ == OperationMode::ReadAppend) or IsWritable();
                }

                bool IsReadable() const {
                        return (om_ == OperationMode::ReadOnly or om_ == OperationMode::ReadAppend or om_ == OperationMode::ReadWrite);
                }

                bool IsWritable() const {
                        return (om_ == OperationMode::WriteOnly or om_ == OperationMode::ReadWrite);
                }

                PlacementId get_placement_id() const {
                        return placement_id_;
                }
                void set_placement_id(const PlacementId& id) {
                        placement_id_ = id;
                }
                uint64_t GetInode() const {
                        return inode_;
                }
        };

private:

        inline const std::string suffix2str(const SuffixType& t) const {
                switch (t) {
                case SuffixType::WAL:
                        return "log";
                case SuffixType::SST:
                        return "sst";
                case SuffixType::MANIFEST:
                        return "MANIFEST";
                case SuffixType::CURRENT:
                        return "CURRENT";
                case SuffixType::IDENTITY:
                        return "IDENTITY";
                case SuffixType::OPTION:
                        return "OPTION";
                case SuffixType::LOG:
                        return "LOG";
                case SuffixType::DBTMP:
                        return "dbtmp";
                case SuffixType::OPTION_DBTMP:
                        return "OPTION_dbtmp";
                case SuffixType::LOG_OLD:
                        return "LOG.old";
                case SuffixType::LOCK:
                        return "LOCK";
                case SuffixType::NONE:
                        return "";
                case SuffixType::LDB:
                        return "ldb";
                case SuffixType::GENERAL:
                        return "";
                case SuffixType::BLOB:
                        return "blob";
                }
                __builtin_unreachable();
        }

        inline SuffixType suffix2type(const std::string& str) const {
                if (str == "log") {
                        return SuffixType::WAL;
                }
                else if (str == "sst") {
                        return SuffixType::SST;
                }
                else if (str == "MANIEST") {
                        return SuffixType::MANIFEST;
                }
                else if (str == "CURRENT") {
                        return SuffixType::CURRENT;
                }
                else if (str == "IDENTITY") {
                        return SuffixType::IDENTITY;
                }
                else if (str == "OPTIONS") {
                        return SuffixType::OPTION;
                }
                else if (str == "LOG") {
                        return SuffixType::LOG;
                }
                else if (str == "dbtmp") {
                        return SuffixType::DBTMP;
                }
                else if (str == "OPTION_dbtmp") {
                        return SuffixType::OPTION_DBTMP;
                }
                else if (str == "LOG.old") {
                        return SuffixType::LOG_OLD;
                }
                else if (str == "LOCK") {
                        return SuffixType::LOCK;
                }
                else if (str == "") {
                        return SuffixType::NONE;
                }
                else if (str == "ldb") {
                        return SuffixType::LDB;
                }
                else if (str == "blob") {
                        return SuffixType::BLOB;
                }
                throw FileNotFound("suffix:" + str);
        }

        inline std::string filename2str(const FileName& f) const {
                auto n = f.name_;
                std::array<char, 256> filename;
                switch (f.suffix_) {
                case SuffixType::MANIFEST:
                        snprintf(filename.data(), filename.size(), "MANIFEST-%06" PRIu64, n);
                        break;
                case SuffixType::OPTION:
                        snprintf(filename.data(), filename.size(), "OPTIONS-%06" PRIu64, n);
                        break;
                case SuffixType::OPTION_DBTMP:
                        snprintf(filename.data(), filename.size(), "OPTIONS-%06" PRIu64 ".dbtmp", n);
                        break;
                case SuffixType::CURRENT:
                        assert(n == 0);
                        snprintf(filename.data(), filename.size(), "CURRENT");
                        break;
                case SuffixType::IDENTITY:
                        assert(n == 0);
                        snprintf(filename.data(), filename.size(), "IDENTITY");
                        break;
                case SuffixType::LOG:
                        assert(n == 0);
                        snprintf(filename.data(), filename.size(), "LOG");
                        break;
                case SuffixType::LOG_OLD:
                        snprintf(filename.data(), filename.size(), "LOG.old.%" PRIu64 , n);
                        break;
                case SuffixType::LOCK:
                        assert(n == 0);
                        snprintf(filename.data(), filename.size(), "LOCK");
                        break;
                case SuffixType::DBTMP:
                        [[fallthrough]];
                case SuffixType::WAL:
                        [[fallthrough]];
                case SuffixType::SST:
                        [[fallthrough]];
                case SuffixType::LDB:
                        [[fallthrough]];
                case SuffixType::BLOB:
                        {
                                const std::string suf = suffix2str(f.suffix_);
                                snprintf(filename.data(), filename.size(), "%06" PRIu64 ".%s", n, suf.c_str());
                        }
                        break;
                case SuffixType::NONE:
                        filename.data()[0] = '\0';
                        break;
                case SuffixType::GENERAL:
                        return dir2str(n);
                }
                return std::string(filename.data());
        }

        inline std::string path2str(const Path& p) const {
                const auto& d = p.dirname_;
                const auto& f = p.filename_;
	
                return dirs2str(d) + filename2str(f);
        }
    
    
        inline const std::string dir2str(const DirType& d) const {
                return dirs_map_.at(d);
        }

        inline const std::string dirs2str(const std::vector<DirType>& dirs) const {
                std::string ret = "/";
                for (const auto& d: dirs) {
                        ret += dir2str(d);
                        ret += "/";
                }
                return ret;
        }

        inline DirType str2dir(const std::string& str) const {
                ASSERT_LOCK_IS_HELD(mtx_);
                for (uint32_t ret = 0; ret < dirs_map_.size(); ret++) {
                        if (dirs_map_[ret] == str) {
                                return ret;
                        }
                }
                dirs_map_.push_back(str);
                return dirs_map_.size() - 1;
        }

        inline std::vector<DirType> str2dirs(const std::string& path) const {
                auto dirs = split_by(path, '/');
                std::vector<DirType> ret;
                for (const auto& d: dirs) {
                        ret.push_back(str2dir(d));
                }
                return ret;
        }

        enum class FileLifeCycle : uint8_t {
                CLOSE = 0,
                OPEN = 1,
                UNUSED = 255,
        };
        static constexpr size_t INDIRECT_BLOCKS_MAX_NUM = 10;

        struct __attribute__((packed)) File {
                uint64_t filename_: 64; // max 64bit
                uint64_t suffix_ : 16;
                uint64_t life_ : 8;
                uint64_t length_ :32; // max 4GB; 32bit
                uint64_t dir_len_: 3; // max 15 directory
                uint64_t dir_ : 64; // 8bit (256 type of directory) * 8 dir = 64bit
                uint64_t indirect_blocks_num_ : 4;
                uint32_t indirect_blocks_[INDIRECT_BLOCKS_MAX_NUM];
                
                File() : filename_(0), suffix_(0), life_(0), length_(0), dir_len_(0), dir_(0), indirect_blocks_num_(0) {}
                File(const std::vector<DirType>& d, uint64_t filename, SuffixType t, uint32_t length)
                        : filename_(filename), suffix_(static_cast<uint32_t>(t)), life_(static_cast<uint32_t>(FileLifeCycle::CLOSE)),
                          length_(length) {
                        assert(d.size() <= 8);

                        dir_len_ = static_cast<uint32_t>(d.size());
                        dir_ = 0;
                        for (size_t i = 0; i < d.size(); i++) {
                                assert(d[i] < 256);
                                dir_ |= (static_cast<uint64_t>(d[i]) << (i*8));
                        }

                        indirect_blocks_num_ = 0;
                        memset(&indirect_blocks_[0], 0, sizeof(indirect_blocks_));
                        
                        // check
                        [[maybe_unused]] const auto p = getPath();
                        assert(d == p.dirname_);
                        assert(FileName(filename, t) == p.filename_);
                }
                File(const Path& p) : life_(static_cast<uint32_t>(FileLifeCycle::CLOSE)), length_(0) {
                        const auto& d_ = p.dirname_;
                        const auto& f_ = p.filename_;
                        filename_ = f_.name_;
                        suffix_ = static_cast<uint32_t>(f_.suffix_);

                        assert(d_.size() <= 8);

                        dir_len_ = static_cast<uint32_t>(d_.size());

                        dir_ = 0;
                        for (size_t i = 0; i < d_.size(); i++) {
                                assert(d_[i] < 256 /*8bit = 256 type of directory*/);
                                dir_ |= (static_cast<uint64_t>(d_[i]) << (i*8));
                        }

                        indirect_blocks_num_ = 0;
                        memset(&indirect_blocks_[0], 0, sizeof(indirect_blocks_));
                        
                        [[maybe_unused]] const auto gp = getPath();
                        assert(p == gp);
                }
                File(const File&) = delete;
                File& operator = (const File& rhs) {
                        if (this == &rhs) {
                                return *this;
                        }
                        filename_ = rhs.filename_;
                        suffix_ = rhs.suffix_;
                        life_ = rhs.life_;
                        length_ = rhs.length_;
                        dir_len_ = rhs.dir_len_;
                        dir_ = rhs.dir_;
                        return *this;
                }
                bool operator == (const File& rhs) const {
                        return ((filename_ == rhs.filename_) and (length_ == rhs.length_) and
                                (suffix_ == rhs.suffix_) and (life_ == rhs.life_)
                                and (dir_len_ == rhs.dir_len_)
                                and (dir_ == rhs.dir_)
                                and (indirect_blocks_num_ == rhs.indirect_blocks_num_));
                }

                Path getPath() const {
                        std::vector<DirType> dirs;
                        for (uint32_t i = 0; i < dir_len_; i++) {
                                DirType d = static_cast<DirType>(static_cast<uint64_t>(dir_ >> (i*8)) & 0xffUL);
                                dirs.push_back(d);
                        }
                        assert(dirs.size() == dir_len_);
	    
                        return { dirs , FileName(static_cast<uint64_t>(filename_), static_cast<SuffixType>(suffix_)) };

                }

                bool is_closed() const {
                        return life_ == static_cast<uint8_t>(FileLifeCycle::CLOSE);
                }

                bool is_unused() const {
                        return life_ == static_cast<uint8_t>(FileLifeCycle::UNUSED);
                }

                bool is_open() const {
                        return !is_closed() and !is_unused();
                }

                void set_closed() {
                        life_ = static_cast<uint8_t>(FileLifeCycle::CLOSE);
                }
                void set_open() {
                        life_ = static_cast<uint8_t>(FileLifeCycle::OPEN);
                }
                void set_unused() {
                        life_ = static_cast<uint8_t>(FileLifeCycle::UNUSED);
                }

                void ref_increase() {
                        assert(life_ != static_cast<uint8_t>(FileLifeCycle::UNUSED));
                        life_++;
                        assert(life_ != static_cast<uint8_t>(FileLifeCycle::UNUSED));
                }
                void ref_decrease() {
                        assert(life_ > 0);
                        assert(life_ != static_cast<uint8_t>(FileLifeCycle::UNUSED));
                        assert(life_ >= static_cast<uint8_t>(FileLifeCycle::OPEN));
                        life_--;
                        assert(life_ >= static_cast<uint8_t>(FileLifeCycle::CLOSE));
                        assert(life_ != static_cast<uint8_t>(FileLifeCycle::UNUSED));
                }
        };
        static_assert(sizeof(File) == 64, "File size");

        static constexpr uint32_t METADATA_SIZE = 32*1024*1024; // 32MB
        static constexpr uint32_t DIRS_COUNT = 256;
        static constexpr uint32_t DIR_LEN = 128;
        static constexpr uint32_t FILE_COUNT = (METADATA_SIZE - sizeof(uint64_t) * 3 - DIR_LEN*DIRS_COUNT) / sizeof(File);
        
        struct alignas(4096) Metadata {
                uint64_t magic;
                uint64_t num_dirs;
                std::array<char[DIR_LEN], DIRS_COUNT> dirs;
                uint64_t num_files;
                std::array<File, FILE_COUNT> files;
                Metadata() : magic(0), num_files(0) {
                }
                ~Metadata() {}
                Metadata(const Metadata& rhs) : magic(rhs.magic), num_files(rhs.num_files), files(rhs.files) {}
                bool operator == (const Metadata& rhs) const {
                        return (magic == rhs.magic and
                                num_dirs == rhs.num_dirs and
                                std::equal(dirs.begin(), dirs.begin() + num_dirs, rhs.dirs.begin()) and
                                num_files == rhs.num_files and
                                std::equal(files.begin(), files.begin() + num_files, rhs.files.begin()));
                }
                Metadata& operator = (const Metadata& rhs) {
                        if (this == &rhs) {
                                return *this;
                        }
                        magic = rhs.magic;
                        num_files = rhs.num_files;
                        std::copy(rhs.files.begin(), rhs.files.begin() + num_files, files.begin());
                        return *this;
                }
        };
        static_assert(sizeof(Metadata) == METADATA_SIZE, "metadata should be 16MB");

        static constexpr size_t GRANULARITY = 4096;
        std::bitset<METADATA_SIZE/GRANULARITY> meta_dirty;

        void set_dirty(const File* ptr) {
                ASSERT_LOCK_IS_HELD(mtx_);
                auto meta_head = reinterpret_cast<uintptr_t>(meta.get());
                auto meta_modified = reinterpret_cast<uintptr_t>(ptr);
                assert(meta_head <= meta_modified);
                const auto bit_index = (meta_modified - meta_head)/GRANULARITY;
                assert(bit_index < meta_dirty.size());
                meta_dirty.set(bit_index);
        }

        void clear_dirty() {
                ASSERT_LOCK_IS_HELD(mtx_);
                meta_dirty.reset();
        }

        void store_dirs_data_to_meta(const std::vector<std::string>& dirs) {
                if (dirs.size() > DIRS_COUNT) {
                        std::cout << __FUNCTION__;
                        for (const auto& d: dirs) {
                                std::cout << " " << d;
                        }
                        std::cout << std::endl;
                }
                assert(dirs.size() <= DIRS_COUNT);
                for (size_t idx = 0; idx < dirs.size(); idx++) {
                        const auto& d = dirs[idx];
                        if (d.size() + 1 > DIR_LEN) {
                                std::cout << d << " is too long, size " << d.size() << std::endl;
                        }
                        assert(d.size() + 1 /* for NULL */ <= DIR_LEN);
                        //std::copy(d.begin(), d.end(), meta->dirs[idx]);
                        strcpy(meta->dirs[idx], d.c_str());
                }
                meta->num_dirs = dirs.size();
        }

        std::vector<std::string> restore_dirs_data_from_meta() {
                std::vector<std::string> ret;
                const auto& num_dirs = meta->num_dirs;
                for (size_t idx = 0; idx < num_dirs; idx++) {
                        assert(strlen(meta->dirs[idx]) < DIR_LEN);
                        ret.emplace_back(meta->dirs[idx]);
                }

                assert(ret.size() <= DIRS_COUNT);
                return ret;
        }

    
        void save_meta() {
                ASSERT_LOCK_IS_HELD(mtx_);

                static_assert(sizeof(*meta) == METADATA_SIZE, "write must be 16MB");
                static_assert(alignof(Metadata) == 4096, "write must be 4kb aligned");

                [[maybe_unused]] const auto max_file = (device_length_ - begin_file_offset_) / block_length_;
                assert(meta->num_files == std::min<uint64_t>(max_file, FILE_COUNT));
	
                const auto meta_head = reinterpret_cast<uintptr_t>(meta.get());
                const auto meta_tail = reinterpret_cast<uintptr_t>(&meta->files.at(meta->num_files));
                assert(meta_head <= meta_tail);
                const auto max_bit_index = (meta_tail - meta_head)/GRANULARITY;
                store_dirs_data_to_meta(dirs_map_);

                // static_assert(offsetof(Metadata, files)/GRANULARITY == 4);
                for (size_t blk_idx = 0; blk_idx < offsetof(Metadata, files)/GRANULARITY; blk_idx++) {
                        meta_dirty.set(blk_idx);
                }

                
                [[maybe_unused]] ssize_t count = 0;
                [[maybe_unused]] ssize_t total_write = 0;
                for (size_t blk_idx = 0; blk_idx <= max_bit_index; blk_idx++) {
                        if (!meta_dirty.test(blk_idx)) {
                                continue;
                        }
                        count += GRANULARITY;
                        total_write += Pwrite(device_, reinterpret_cast<char*>(meta.get()) + blk_idx*GRANULARITY, GRANULARITY, blk_idx*GRANULARITY, PlacementId::METADATA);
                        assert((blk_idx + 1) * GRANULARITY <= sizeof(Metadata));
                }
                assert(count == total_write);
                assert(total_write <= METADATA_SIZE);
                clear_dirty();
        }

        bool load_meta() {
                // read the head of metadata
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                ssize_t ret = Pread(device_, meta.get(), sizeof(Metadata), 0);
                if (ret != sizeof(Metadata)) {
                        std::cout << "fail read metadata from device " << ret << std::endl;
                        return false;
                }

                if (meta->magic != FdpFsMagic) {
                        printf("BAD magic actual 0x%lx not 0x%lx\n", meta->magic, FdpFsMagic);
                        return false;
                }

                const auto max_file = (device_length_ - begin_file_offset_) / block_length_;
                if (meta->num_files != std::min<uint64_t>(max_file, FILE_COUNT)) {
                        std::cout << "missmatch filenums actual " << meta->num_files << " not " << std::min<uint64_t>(max_file, FILE_COUNT) << std::endl;
                        return false;
                }

                dirs_map_ = restore_dirs_data_from_meta();
	
                // calc total read size
                const auto& read_size = round_up<uint64_t>(offsetof(Metadata, files) + sizeof(File) * meta->num_files, GRANULARITY);
                ret = Pread(device_, reinterpret_cast<char*>(meta.get()) + GRANULARITY, read_size - GRANULARITY, GRANULARITY);
                if (ret < 0 || static_cast<uint64_t>(ret) != read_size - GRANULARITY) {
                        std::cout << "read file metadata failed" << std::endl;
                        return false;
                }

                
                std::set<uint32_t> indirect_blocks;
                for (uint32_t phys_index = 0; phys_index < meta->num_files; phys_index++) {
                        auto& f = meta->files.at(phys_index);
                        if (f.is_open() or f.is_closed()) {
                                meta_in_mem.emplace(f.getPath(), phys_index);
                                for (size_t i = 0; i < f.indirect_blocks_num_; i++) {
                                        indirect_blocks.insert(f.indirect_blocks_[i]);
                                }
                                if ((f.length_ / block_length_) != f.indirect_blocks_num_) {
                                        std::cout << path2str(f.getPath()) << " f.length_ " << f.length_ << " block_length_ "
                                                  << block_length_ << "  indirect_blocks_num " << f.indirect_blocks_num_ << std::endl;
                                }
                                assert((f.length_ / block_length_)  == f.indirect_blocks_num_);
                        }
                        else {
                                assert(f.is_unused());
                                free_blocks.insert(phys_index, false);
                        }
                        
                }
                // remove indirect blocks from free blocks
                for (uint32_t b: indirect_blocks) {
                        free_blocks.erase(b);
                }

                return true;
        }

        File* get_file(const FdpFsFd& fd) const {
                ASSERT_LOCK_IS_HELD(mtx_);
                File* ret = &meta->files.at(fd.inode_);
                return ret;
        }

        File* get_file(const Path& f) const {
                ASSERT_LOCK_IS_HELD(mtx_);
                const auto& itr = meta_in_mem.find(f);
                if (itr == meta_in_mem.end()) {
                        //std::cout << boost::stacktrace::stacktrace() << std::endl;
                        throw FileNotFound("get_file " + path2str(f));
                }
                return &meta->files.at(itr->second);
        }

        File* get_file(const std::string& path) const {
                ASSERT_LOCK_IS_HELD(mtx_);
                return get_file(fileno(path));
        }

        void update(const FdpFsFd& fd) {
                ASSERT_LOCK_IS_HELD(mtx_);

                File* f = get_file(fd);
                assert(f != nullptr);

                if (fd.closed_) {
                        // fd close
                        Close(f);

                        // if no name in meta_in_mem; then free File meta and data block
                        if (f->is_closed() and meta_in_mem.find(fd.file_) == meta_in_mem.end()) {
                                assert(!free_blocks.contains(fd.inode_));
                                free_blocks.insert(fd.inode_);
                                DiscardSegment(device_, begin_file_offset_ + fd.inode_ * block_length_, block_length_);

                                for (size_t i = 0; i < f->indirect_blocks_num_; i++) {
                                        uint32_t b = f->indirect_blocks_[i];
                                        assert(!free_blocks.contains(b));
                                        free_blocks.insert(b);
                                        DiscardSegment(device_, begin_file_offset_ + b * block_length_, block_length_);
                                }
                                Remove(f);
                        }
                        
                        // if (f->length_ == fd.length_) {
                        //         return;
                        // }
                        // std::cout << path2str(fd.file_) << " close and save blocks size " << fd.blocks_.size() << std::endl;
                        // close operation
                        f->length_ = fd.length_;
                        assert(static_cast<size_t>(f->indirect_blocks_num_ + 1) <= fd.blocks_.size());
                        assert(fd.blocks_.size() <= INDIRECT_BLOCKS_MAX_NUM + 1);
#ifndef NDEBUG
                        for (size_t i = 0; i < f->indirect_blocks_num_; i++) {
                                uint64_t inb = begin_file_offset_ + block_length_ * f->indirect_blocks_[i];                                
                                assert(inb == fd.blocks_[i + 1]);
                                assert(!free_blocks.contains(f->indirect_blocks_[i]));
                        }
#endif
                        f->indirect_blocks_num_ = fd.blocks_.size() - 1;
                        for (size_t i = 0; i < fd.blocks_.size() - 1; i++) {
                                assert(((fd.blocks_[i + 1] - begin_file_offset_) % block_length_) == 0);
                                uint32_t inb = (fd.blocks_[i + 1] - begin_file_offset_) / block_length_;
                                f->indirect_blocks_[i] = inb;
                                assert(!free_blocks.contains(f->indirect_blocks_[i]));                                
                        }

                        set_dirty(f);

                        return;
                }

                // update length metadata
                if (f->is_open()) {
                        // assert(f->is_open());

                        assert(fd.length_ <= block_length_ * (INDIRECT_BLOCKS_MAX_NUM + 1));
                        //assert(f->length_ <= fd.length_); // now, the file size can be zero due to trunc and positioned append

                        // if (f->length_ == fd.length_) {
                        //         return;
                        // }
                        f->length_ = fd.length_;
                        assert(static_cast<size_t>(f->indirect_blocks_num_ + 1) <= fd.blocks_.size());
                        assert(fd.blocks_.size() <= INDIRECT_BLOCKS_MAX_NUM + 1);
#ifndef NDEBUG                        
                        for (size_t i = 0; i < f->indirect_blocks_num_; i++) {
                                uint64_t inb = begin_file_offset_ + block_length_ * f->indirect_blocks_[i];                                            assert(inb == fd.blocks_[i + 1]);
                                assert(!free_blocks.contains(f->indirect_blocks_[i]));                                
                        }
#endif
                        f->indirect_blocks_num_ = fd.blocks_.size() - 1;                        
                        for (size_t i = 0; i < fd.blocks_.size() - 1; i++) {
                                assert(((fd.blocks_[i + 1] - begin_file_offset_) % block_length_) == 0);
                                uint32_t inb = (fd.blocks_[i + 1] - begin_file_offset_) / block_length_;
                                f->indirect_blocks_[i] = inb;
                                assert(!free_blocks.contains(f->indirect_blocks_[i]));                                
                        }
                        set_dirty(f);
                }
        }

        FileName filename_parse(const std::string& fname) const {
                const std::regex regex_fname_num("[0-9]+\\.[a-zA-Z0-9]+");
                if (fname.empty()) {
                        return FileName(0, SuffixType::NONE);
                }
                else if (fname == "LOCK") { // lock
                        return FileName(0, SuffixType::LOCK);
                }
                else if (fname.at(0) == 'M') { // "MANIFEST-0000"
                        errno = 0;
                        uint64_t name = strtoull(fname.c_str() + strlen("MANIFEST-"), nullptr, 10);
                        assert(errno == 0);
                        return FileName(name, SuffixType::MANIFEST);
                }
                else if (fname.at(0) == 'O') { // "OPTIONS-0000"
                        errno = 0;
                        uint64_t name = strtoull(fname.c_str() + strlen("OPTIONS-"), nullptr, 10);
                        assert(errno == 0);
                        bool tmp = fname.back() == 'p'; // OPTIONS-00001.dbtmp
                        return FileName(name, tmp ? SuffixType::OPTION_DBTMP : SuffixType::OPTION);
                }
                else if (fname.at(0) == 'C') { // CURRENT
                        return FileName(0, SuffixType::CURRENT);
                }
                else if (fname.at(0) == 'I') { // IDENTITY
                        return FileName(0, SuffixType::IDENTITY);
                }
                else if (fname == "LOG") {
                        return FileName(0, SuffixType::LOG);
                }
                else if (fname.substr(0, 8) == "LOG.old.") {
                        errno = 0;
                        uint64_t num = strtoull(fname.c_str() + strlen("LOG.old."), nullptr, 10);
                        assert(errno == 0);
                        return FileName(num, SuffixType::LOG_OLD);
                } else if (std::regex_match(fname,
                                            regex_fname_num)) {  // 0000000.XXX
                        errno = 0;
                        uint64_t name = strtoull(fname.c_str(), nullptr, 10);
                        assert(errno == 0);
                        auto suf = split_by(fname, '.');
                        assert (suf.size() == 2);
                        return FileName(name, suffix2type(suf[1]));
                } else {
                        uint64_t n = str2dir(fname);
                        return FileName(n, SuffixType::GENERAL);
                }
        }

    
        /* str <-> Path */
        Path fileno(const std::string& path) const {
                if (path.at(0) != '/') {
                        std::cout << "fileno invalid path: " << path << std::endl;
                }
                assert(path.at(0) == '/');
                auto strs = split_by(path, '/');

                auto fname_str = strs.back();
                strs.pop_back();

                FileName fname = filename_parse(fname_str);

                std::vector<DirType> dirs;
                for (const auto& d: strs) {
                        if (d.empty()) {
                                continue;
                        }
                        dirs.push_back(str2dir(d));
                }

                return Path(dirs, fname);
        }

        Path fileno(const char* path) const {
                return fileno(std::string(path));
        }

        std::tuple<uint64_t, uint64_t, uint64_t>
        device_open(int* dev, const char* path) const {
                int fd = ::open(path, O_DIRECT | O_RDWR);
                if (fd < 0) {
                        std::cerr << "FdpFs opening " << path << " failed: " << std::strerror(errno) << std::endl;
                        throw std::runtime_error("open");
                }

                uint64_t dev_len;
                auto n = ioctl(fd, BLKGETSIZE64, &dev_len);
                if (n < 0) {
                        std::cerr << "ioctl failed: " << std::strerror(errno) << std::endl;
                        throw std::runtime_error("ioctl");
                }
                *dev = fd;

                constexpr uint64_t max_len = 256*1024*1024UL;
                constexpr uint64_t begin_off = sizeof(*meta);

                return { dev_len, max_len, begin_off };
        }

        std::tuple<uint64_t, uint64_t, uint64_t>
        device_open(RingContext* dev, const char* path) const {
                const char* use_fdp_str = getenv("USE_FDP");
                const bool use_fdp = use_fdp_str != nullptr and (strcasecmp(use_fdp_str, "true") == 0 or
                                                                 strcasecmp(use_fdp_str, "yes") == 0 or
                                                                 strcasecmp(use_fdp_str, "enable") == 0);

                const char* use_discard_str = getenv("USE_DISCARD");
                const bool use_discard = use_discard_str != nullptr and (strcasecmp(use_discard_str, "true") == 0 or
                                                                 strcasecmp(use_discard_str, "yes") == 0 or
                                                                 strcasecmp(use_discard_str, "enable") == 0);

                new(dev) RingContext(128, path, use_fdp, use_discard);
                const uint64_t dev_len = dev->dev_len();

                constexpr uint64_t max_len = 256*1024*1024UL;
                constexpr uint64_t begin_off = sizeof(*meta);

                return { dev_len, max_len, begin_off };
        }


        void device_close(int* fd) const {
                int n = ::close(*fd);
                if (n < 0) {
                        std::cerr << "FdpFs closing failed: " << std::strerror(errno) << std::endl;
                }
        }

        void device_close(RingContext* dev) const {
                dev->close();
        }

        static constexpr uint64_t FdpFsMagic = 0x123fbeef;
        //uint64_t each_file_max_length_;
        uint64_t block_length_;

        std::unique_ptr<Metadata> meta;

        struct KeyHash {
                inline std::size_t operator()(const Path& p) const
                {
                        const auto& d = p.dirname_;
                        const auto& f = p.filename_;
                        return static_cast<std::size_t>(d.size()) + 
                                static_cast<std::size_t>(f.name_) + static_cast<std::size_t>(f.suffix_);
                }
        };

        template<typename U> class FreeBlockSet {
                std::set<U> free_blocks_;
        public:
                using size_type = typename std::set<U>::size_type;
                size_type size() const {
                        return free_blocks_.size();
                }
                U get_and_erase() {
                        if (free_blocks_.empty()) {
                                throw NoSpace();
                        }
                        auto t = *free_blocks_.begin();
                        free_blocks_.erase(t);
                        return t;
                }
                void erase(const U& elm) {
                        free_blocks_.erase(elm);
                }
                void insert(const U& elm, [[maybe_unused]] bool log = true) {
                        assert(free_blocks_.find(elm) == free_blocks_.end());
                        free_blocks_.emplace(elm);
                }
                void clear() {
                        free_blocks_.clear();
                }
                bool contains(const U& elm) const {
                        return free_blocks_.find(elm) != free_blocks_.end();
                }
        };
        void check_free_blocks() const {
                return;
                for (uint32_t blk = 0; blk < meta->num_files; blk++) {
                        const File* f = &meta->files.at(blk);
                        if (free_blocks.contains(blk)) {
                                if (!f->is_unused()) {
                                        std::cout << __FUNCTION__ << " Freeblock "  << blk << " is not unused mark!" << std::endl;
                                }
                                assert(f->is_unused());
                        }
                        else {
                                // allocated block
                                if (f->is_unused()) {
                                        std::cout << __FUNCTION__ << " Alloblock "  << blk << " is not used mark!" << std::endl;
                                }
                                assert(!f->is_unused());
                        }

                }
        }

        uint64_t device_length_;
        FreeBlockSet<uint32_t> free_blocks;
        std::unordered_map<Path, uint32_t, KeyHash> meta_in_mem;
        mutable std::mutex mtx_;
        // spinlock mtx_;
        T device_;
        uint64_t begin_file_offset_;
        int current_open_ = 0;
public:
        FdpFs(const char* path) : meta(std::make_unique<Metadata>()) {
                const auto& [dev_len, max_len, begin_off] = device_open(&device_, path);
                device_length_ = dev_len;
                //each_file_max_length_ = max_len;
                block_length_ = max_len;
                begin_file_offset_ = begin_off;

                assert(sizeof(Metadata) <= begin_file_offset_);
                assert(sizeof(Metadata) < device_length_);

                if (!load_meta()) {
                        std::cerr << "fail load fs metadata, make file system..." << std::endl;
                        Mkfs();
                }

                auto max_file = (device_length_ - begin_file_offset_) / block_length_;

                std::cout << "FdpFS Open "  << path << ", " << device_length_ << " bytes, max number of files by device size " << max_file
                          << ", max number of files by metadata size " << FILE_COUNT << ", max file size " << block_length_ * (INDIRECT_BLOCKS_MAX_NUM + 1) << " bytes, free block num " << free_blocks.size()<< ", file start at " << begin_file_offset_ << std::endl;

                [[maybe_unused]] auto ret = CheckFs();
                assert(ret);
        }

        ~FdpFs() {
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                std::cout << "FdpFS closing..." << std::endl;
                save_meta();

                device_close(&device_);
                assert(meta_dirty.none());
        }

        bool CheckFs() {
                std::lock_guard<decltype(mtx_)> lock(mtx_);

                auto ret = (meta->magic == FdpFsMagic);
                if (!ret) {
                        std::cout << "invalid magic" << std::endl;
                }

                [[maybe_unused]] const auto max_file = (device_length_ - begin_file_offset_) / block_length_;
                assert(meta->num_files == std::min<uint64_t>(FILE_COUNT, max_file));

                for (uint32_t blk = 0; blk < meta->num_files; blk++) {
                        auto& f = meta->files.at(blk);
                        if (f.is_open()) {
                                // std::cout << "unclosed file found " << path2str(f.getPath()) << std::endl;
                                // const auto& allocated_lba = begin_file_offset_ + each_file_max_length_*blk;
                                // CloseSegment(device_, allocated_lba, each_file_max_length_);
                                Close(&f);
                                assert(f.is_closed());
                        }
                        if (!f.is_unused()) {
                                if (free_blocks.contains(blk)) {
                                        std::cout << "invalid direct blocks" << std::endl;
                                        ret = false;
                                }
                                if (f.length_ > block_length_ * (INDIRECT_BLOCKS_MAX_NUM + 1) ||
                                    (f.length_ / block_length_) != f.indirect_blocks_num_) {
                                        std::cout << "invalid file size" << std::endl;
                                        ret = false;
                                }
                                for (size_t i = 0; i < f.indirect_blocks_num_; i++) {
                                        if (free_blocks.contains(f.indirect_blocks_[i])) {
                                                std::cout << "invalid indirect blocks" << std::endl;
                                                ret = false;
                                        }
                                }
                        }
                }

                return ret;
        }

        std::vector<std::string> Readdir(const std::string& dir) const {
                try {
                        // const auto& p = fileno(dir);
                        std::lock_guard<decltype(mtx_)> lock(mtx_);
                        const auto& dirs = str2dirs(dir);
                        const auto& ret = ReaddirSub(dirs);
                        return std::vector<std::string>(ret.begin(), ret.end());
                } catch (FileNotFound& e) {
                        return {};
                }
        }

        std::unordered_set<std::string> ReaddirSub(const std::vector<DirType>& target_dirs) const {
                ASSERT_LOCK_IS_HELD(mtx_);
	
                std::unordered_set<std::string> ret;
                check_free_blocks();
                for (const auto& f: meta_in_mem) {
#ifndef NDEBUG
                        File* fptr = get_file(f.first);
                        assert(fptr != nullptr);
                        if (fptr->getPath() != f.first) {
                                std::cout << path2str(fptr->getPath()) << " " << path2str(f.first) << std::endl;
                        }
	    
                        assert(fptr->getPath() == f.first);
#endif
                        const auto& [dir, fil] = f.first;

                        if (dir.size() < target_dirs.size()) {
                                continue;
                        }
	    
                        assert(dir.size() >= target_dirs.size());
                        bool retry = false;
                        for (size_t i = 0; i < target_dirs.size(); i++) {
                                if (dir[i] != target_dirs[i]) {
                                        retry = true;
                                        break;
                                }
                        }
                        if (retry) {
                                continue;
                        }

                        if (dir.size() == target_dirs.size()) {
                                ret.insert(filename2str(fil));
                        }
                        else {
                                assert(dir.size() > target_dirs.size());
                                ret.insert(dir2str(dir[target_dirs.size()]));
                        }
                }

                return ret;
        }

        FdpFsFd Open(const std::string& path, OperationMode om, CreateMode cm, PlacementId placement_id = PlacementId::NONE) {
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                if (current_open_ > 1024*1024) {
                        std::cout << "Too many open files" << std::endl;
                        throw TooManyOpenFiles("Open Failed, " + path);
                }
                
                current_open_++;

                check_free_blocks();
                auto fno = fileno(path);
                auto f = meta_in_mem.find(fno);
                bool notfound = (f == meta_in_mem.end());
                // existing file
                switch (cm) {
                case CreateMode::CreateIfMissing:
                        if (notfound) { // create
                                // make file with one freeblock
                                        auto free_block =
                                            free_blocks.get_and_erase();
                                        meta_in_mem.emplace(fno, free_block);
                                        assert(meta_in_mem.find(fno) !=
                                               meta_in_mem.end());
                                        File* fptr = get_file(fno);
                                        assert(fptr != nullptr);
                                        assert(fptr->is_unused());
                                        new (fptr) File(fno);
                                        assert(fptr->is_closed());
                                        assert(fptr->getPath() == fno);
                                        set_dirty(fptr);

                                        f = meta_in_mem.find(fno);
                                        assert(f != meta_in_mem.end());

                                        const auto& allocated_lba =
                                                begin_file_offset_ +
                                                block_length_ * free_block;
                                        DiscardSegment(device_, allocated_lba,
                                                       block_length_);
                        }
                        break;
                case CreateMode::ErrorIfMissing:
                        if (notfound) {
                                throw FileNotFound("Create ErrorIfMissing " + path);
                        }
                        break;
                }
                File* fptr = get_file(fno);
                assert(fptr != nullptr);

                Open(fptr);
                // std::cout << path << std::endl;
                return FdpFsFd(*this, *fptr, f->second, om, placement_id);
        }

        void Open(File* f) {
                assert(!f->is_unused());
                f->ref_increase();
                set_dirty(f);
        }

        void Mkfs() {
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                std::memset(static_cast<void*>(meta.get()), 0, METADATA_SIZE);
                meta->magic = FdpFsMagic;
                const auto max_file = (device_length_ - begin_file_offset_) / block_length_;
                meta->num_files = std::min<uint64_t>(FILE_COUNT, max_file);

                free_blocks.clear();

                for (uint32_t blk = 0; blk < meta->num_files; blk++) {
                        Remove(&meta->files.at(blk));
                        free_blocks.insert(blk, false);
                        DiscardSegment(device_, begin_file_offset_ + block_length_*blk, block_length_);
                }

                meta_in_mem.clear();

                save_meta();
        }

        void Remove(const Path& filename) {
                ASSERT_LOCK_IS_HELD(mtx_);
                auto itr = meta_in_mem.find(filename);
                if (itr == meta_in_mem.end()) {
                        throw FileNotFound("Remove " + path2str(filename));
                }

                // delete actual file
                File* f = get_file(filename);
                assert(f == &meta->files.at(itr->second));

                assert(!f->is_unused());

                if (f->is_closed()) {
                        Remove(f);
                        assert(!free_blocks.contains(itr->second));
                        free_blocks.insert(itr->second);
                        DiscardSegment(device_, begin_file_offset_ + itr->second * block_length_, block_length_);

                        //remove indirect blocks
                        for (size_t i = 0; i < f->indirect_blocks_num_; i++) {
                                uint32_t b =  f->indirect_blocks_[i];
    
                                assert(!free_blocks.contains(b));
                                free_blocks.insert(b);
                                DiscardSegment(device_, begin_file_offset_ + b * block_length_, block_length_);                                
                        }
                }

                // remove name
                meta_in_mem.erase(itr);
        }

        void Remove(File* f) {
                f->set_unused();
                set_dirty(f);
        }

        void Remove(const std::string& path) {
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                auto filename = fileno(path);
                Remove(filename);
        }

        bool Exists(const char* path) {
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                try {
                        File* f = get_file(path);
                        return (f != nullptr);
                }
                catch (FileNotFound& e) {
                        return false;
                }
        }

        bool Exists(const std::string& path) {
                try {
                        return Exists(path.c_str());
                }
                catch (FileNotFound& e) {
                        return false;
                }
        }

        uint64_t FileSize(const std::string& path) {
                return FileSize(path.c_str());
        }

        uint64_t FileSize(const char* path) {
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                File* f = get_file(path);
                assert(f != nullptr);
                return f->length_;
        }

        uint64_t FileSize(const FdpFsFd& fd) const {
                CHECK_FD(fd);
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                // assert(fd.length_ == get_file(fd)->length_);
                return fd.length_;
        }

        uint64_t MaxFileSize(const FdpFsFd& fd) const {
                CHECK_FD(fd);
                return block_length_ * (INDIRECT_BLOCKS_MAX_NUM + 1);
        }

        void Truncate(FdpFsFd& fd, uint64_t size) {
                CHECK_FD(fd);
                if (!fd.IsWritable()) {
                        throw InvalidOperation(path2str(fd.file_) + ".Truncate");
                }
                assert(fd.length_ <= block_length_ * (INDIRECT_BLOCKS_MAX_NUM + 1));
                if (size > block_length_ * (INDIRECT_BLOCKS_MAX_NUM + 1)) {
                        throw InvalidOperation(path2str(fd.file_) + ".Truncate size");
                }

                fd.length_ = size;

                std::lock_guard<decltype(mtx_)> lock(mtx_); // for free_blocks

                while (fd.length_  / block_length_ + 1 > fd.blocks_.size()) {
                        auto free_block = free_blocks.get_and_erase();
                        auto b = begin_file_offset_ +
                                block_length_ * free_block;
                        fd.blocks_.push_back(b);
                }
                assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());
                CHECK_FD(fd);
                
#ifndef NDEBUG
                [[maybe_unused]] File* f = get_file(fd);
                assert(f != nullptr);
                assert(f->is_open() or f->is_closed());
#endif
                update(fd);
        }

        template<typename U>
        size_t PositionedAppend(FdpFsFd& fd, const U& buf, uint64_t offset) {
                CHECK_FD(fd);	
                // Truncate(fd, offset);
                Seek(fd, offset, SeekWhence::SET);
                CHECK_FD(fd);	
                return Append(fd, buf);
        }
  
        template<typename U>
        size_t Append(FdpFsFd& fd, const U& buf, size_t actual) {
                if (!fd.IsAppendable()) {
                        throw InvalidOperation(path2str(fd.file_) + ".Append(actual)");
                }

                assert(actual <= buf.size());
                if (fd.cur_  + actual > block_length_ * (INDIRECT_BLOCKS_MAX_NUM)) {
                        throw NoSpace();
                }

                while (((fd.cur_ + buf.size()) / block_length_) + 1 > fd.blocks_.size()) {
                        std::lock_guard<decltype(mtx_)> lock(mtx_);
                        auto free_block = free_blocks.get_and_erase();
                        auto b = begin_file_offset_ +
                                block_length_ * free_block;
                        fd.blocks_.push_back(b);
                }
                // std::cout << path2str(fd.file_) << " blocks "  << fd.blocks_.size() << std::endl;
                
                assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());                
                CHECK_FD(fd);
                uint64_t actual_size = round_up<uint64_t>(actual, 4096);
                assert((actual_size % 4096) == 0);
                assert(actual_size <= buf.size());

                assert(((fd.cur_ / block_length_) == ((fd.cur_ + actual_size) / block_length_)) or
                       ((fd.cur_ / block_length_) + 1 == ((fd.cur_ + actual_size) / block_length_)));

                if ((fd.cur_ / block_length_) < (fd.cur_ + actual_size) / block_length_) {
                        // split case

                        size_t head_idx = fd.cur_ / block_length_;
                        size_t tail_idx = (fd.cur_ + buf.size()) / block_length_;
                        assert(head_idx + 1 == tail_idx);
                        assert(head_idx < fd.blocks_.size());
                        assert(tail_idx < fd.blocks_.size());

                        uint64_t split_head_block = fd.blocks_[head_idx];
                        uint64_t split_head_block_off = fd.cur_ % block_length_;
                        uint64_t split_head_block_len = block_length_ - split_head_block_off;

                        uint64_t split_tail_block = fd.blocks_[tail_idx];
                        uint64_t split_tail_block_off = 0;
                        uint64_t split_tail_block_len = actual_size - split_head_block_len;
                        assert(actual_size >= split_head_block_len);
                        assert((split_head_block_len % 4096) == 0);
                        assert(split_head_block_len < 32UL*1024*1024*1024*1024UL);
                        assert(split_tail_block_len < 32UL*1024*1024*1024*1024UL);
                        Pwrite(fd.device_, buf.data(), split_head_block_len, split_head_block + split_head_block_off, fd.placement_id_);
                        Pwrite(fd.device_, buf.data() + split_head_block_len, split_tail_block_len, split_tail_block + split_tail_block_off, fd.placement_id_);
                        fd.cur_ += round_down<uint64_t>(actual, 4096);
                        auto rem = actual - round_down<uint64_t>(actual, 4096);
                        fd.length_ = fd.cur_ + rem;
                }
                else {
                        size_t block_idx = fd.cur_ / block_length_;
                        size_t block_rem = fd.cur_ % block_length_;
                        assert(block_idx < fd.blocks_.size());
                        assert(actual_size < 32UL*1024*1024*1024*1024UL);
                        Pwrite(fd.device_, buf.data(), actual_size, fd.blocks_[block_idx] + block_rem, fd.placement_id_);

                        auto rem = actual - round_down<uint64_t>(actual, 4096);
                        // advance write head by 4KB step
                        fd.cur_ += round_down<uint64_t>(actual, 4096);
                        fd.length_ = fd.cur_  + rem;

                        CHECK_FD(fd);
                }

                std::lock_guard<decltype(mtx_)> lock(mtx_);
                update(fd);
                return actual;
        }
  
        template<typename U>
        size_t Append(FdpFsFd& fd, const U& buf) {
                if (!fd.IsAppendable()) {
                        throw InvalidOperation(path2str(fd.file_) + ".Append");
                }
                
                if (fd.cur_ + buf.size() > block_length_ * (INDIRECT_BLOCKS_MAX_NUM + 1)) {
                        std::cout << path2str(fd.file_) << " append nospace size " << fd.cur_ << " append buf " << buf.size() << std::endl;
                        throw NoSpace();
                }

                while (((fd.cur_ + buf.size()) / block_length_) + 1 > fd.blocks_.size()) {
                        std::lock_guard<decltype(mtx_)> lock(mtx_);
                        auto free_block = free_blocks.get_and_erase();
                        auto b = begin_file_offset_ +
                                block_length_ * free_block;
                        fd.blocks_.push_back(b);
                }
                assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());                
                CHECK_FD(fd);
                // std::cout << path2str(fd.file_) << " blocks "  << fd.blocks_.size() << std::endl;
                assert((buf.size() % 4096) == 0);
                assert(buf.size() < block_length_);

                assert (((fd.cur_ / block_length_) == ((fd.cur_ + buf.size()) / block_length_)) or
                        ((fd.cur_ / block_length_) + 1 == ((fd.cur_ + buf.size()) / block_length_)));
                                                                                        
                
                if ((fd.cur_ / block_length_) < (fd.cur_ + buf.size()) / block_length_) {
                        // split case

                        size_t head_idx = fd.cur_ / block_length_;
                        size_t tail_idx = (fd.cur_ + buf.size()) / block_length_;
                        assert(head_idx + 1 == tail_idx);
                        assert(head_idx < fd.blocks_.size());
                        assert(tail_idx < fd.blocks_.size());

                        uint64_t split_head_block = fd.blocks_[head_idx];
                        uint64_t split_head_block_off = fd.cur_ % block_length_;
                        uint64_t split_head_block_len = block_length_ - split_head_block_off;

                        uint64_t split_tail_block = fd.blocks_[tail_idx];
                        uint64_t split_tail_block_off = 0;
                        uint64_t split_tail_block_len = buf.size() - split_head_block_len;

                        assert(split_head_block_len < 32UL*1024*1024*1024*1024UL);
                        assert(split_tail_block_len < 32UL*1024*1024*1024*1024UL);
                        Pwrite(fd.device_, buf.data(), split_head_block_len, split_head_block + split_head_block_off, fd.placement_id_);
                        Pwrite(fd.device_, buf.data() + split_head_block_len, split_tail_block_len, split_tail_block + split_tail_block_off, fd.placement_id_);
                        fd.cur_ += buf.size();
                        // fd.length_ = fd.cur_;
                        if (fd.cur_ > fd.length_) {
                                fd.length_ = fd.cur_;
                        }
                }
                else {
                        uint64_t block_idx = fd.cur_ / block_length_;
                        assert(block_idx < fd.blocks_.size());
                        assert(buf.size() < 32UL*1024*1024*1024*1024UL);
                        Pwrite(fd.device_, buf.data(), buf.size(), fd.blocks_[block_idx] + (fd.cur_ % block_length_), fd.placement_id_);
                        fd.cur_ += buf.size();
                        if (fd.cur_ > fd.length_) {
                                fd.length_ = fd.cur_;
                        }
                }
                CHECK_FD(fd);
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                update(fd);
                CHECK_FD(fd);
                return buf.size();
        }

        template<typename U>
        size_t Read(FdpFsFd& fd, U& buf) const {
                if (!fd.IsReadable()) {
                        throw InvalidOperation(path2str(fd.file_) + ".Read");
                }
                CHECK_FD(fd);
                assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());
                auto ret = Read(fd, buf, fd.cur_);
                fd.cur_ += ret;
                CHECK_FD(fd);
                assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());
                return ret;
        }

        template<typename U>
        size_t Read(FdpFsFd& fd, U& buf, uint64_t off) const {
                if (!fd.IsReadable()) {
                        throw InvalidOperation(path2str(fd.file_) + ".Read(offset)");
                }
                assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());
                CHECK_FD(fd);
                // std::cout << path2str(fd.file_) << " blocks "  << fd.blocks_.size() << std::endl;
                if (off > fd.length_) {
                        // noread
                        return 0;
                }
                assert(off <= fd.length_);
                assert(buf.size() <= block_length_);
                assert (((off / block_length_) == (off + buf.size()) / block_length_) or
                        ((off / block_length_) + 1 == ((off + buf.size()) / block_length_)));
                                                                                        
                
                if ((off / block_length_) < (off + buf.size()) / block_length_) {
                        // split case
                        size_t head_idx = off / block_length_;
                        size_t tail_idx = (off + buf.size()) / block_length_;
                        assert(head_idx + 1 == tail_idx);
                        assert(head_idx < fd.blocks_.size());
                        assert(tail_idx < fd.blocks_.size());

                        uint64_t split_head_block = fd.blocks_[head_idx];
                        uint64_t split_head_block_off = off % block_length_;
                        uint64_t split_head_block_len = block_length_ - split_head_block_off;

                        uint64_t split_tail_block = fd.blocks_[tail_idx];
                        uint64_t split_tail_block_off = 0;
                        uint64_t split_tail_block_len = buf.size() - split_head_block_len;
                        
                        assert(split_head_block_len < 32UL*1024*1024*1024*1024);
                        assert(split_tail_block_len < 32UL*1024*1024*1024*1024);
                        Pread(fd.device_, buf.data(), split_head_block_len, split_head_block + split_head_block_off);
                        Pread(fd.device_, buf.data() + split_head_block_len, split_tail_block_len, split_tail_block + split_tail_block_off);
                        return buf.size();
                        /*
                               |---buf.size()-|
                          |----------|----------|----------|
                               |-----|
                              off
                         */
                }
                else {
                        // a case of reading only one block
                        assert(off <= fd.length_);
                        size_t block_idx = off / block_length_;
                        if (block_idx >= fd.blocks_.size()) {
                                // /db/000009.sst filesize 50847635 read off 50384896 block_length_ 33554432 block_idx 1 bocks.size 1
                                std::cout << path2str(fd.file_) << " filesize " << fd.length_ <<  " read off "  << off << " block_length_ " << block_length_ << " block_idx " << block_idx << " bocks.size " << fd.blocks_.size() << std::endl;
                        }
                        assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());                                        
                        assert(block_idx < fd.blocks_.size());

                        Pread(fd.device_, buf.data(), buf.size(), fd.blocks_[block_idx] + (off % block_length_));
                        size_t ex = 0;
                        if (fd.length_ < off + buf.size()) {
                                ex = off + buf.size() - fd.length_;
                        }
                        assert(buf.size() >= ex); // return value >= 0
                        return buf.size() - ex;
                }
        }

        enum class SeekWhence {
                SET, CUR
        };

        uint64_t Seek(FdpFsFd& fd, uint64_t offset, SeekWhence whence) const {
                assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());
                CHECK_FD(fd);
                switch (whence) {
                case SeekWhence::SET:
                        if (offset > fd.length_) {
                                offset = fd.length_;
                        }
                        fd.cur_ = offset;
                        break;
                case SeekWhence::CUR:
                        if (offset + fd.cur_ > fd.length_) {
                                offset = fd.length_ - fd.cur_;
                        }
                        fd.cur_ += offset;
                        break;
                }
                assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());
                CHECK_FD(fd);
                return fd.cur_;
        }

        void Close(FdpFsFd& fd) {
                CHECK_FD(fd);
                assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                fd.closed_ = true;
                update(fd);
                current_open_--;
        }

        bool Closed(const FdpFsFd& fd) const {
                return fd.closed_;
        }

        void Close(File* f) {
                // nothing for closed file
                if (f->is_open()) {
                        f->ref_decrease();
                        set_dirty(f);
                }
        }

        bool Eof(const FdpFsFd& fd) const {
                assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());
                CHECK_FD(fd);
                return (fd.cur_ == fd.length_);
        }

        std::string PathStr(const FdpFsFd& fd) const {
                assert((fd.length_ / block_length_) + 1 <= fd.blocks_.size());
                CHECK_FD(fd);
                return path2str(fd.file_);
        }

        void Sync() {
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                save_meta();
                Fsync(device_);
        }

        void Rename(const std::string& src, const std::string& dst) {
                // const auto& src_sub = Readdir(src);
                // const auto& dst_sub = Readdir(dst);
                // bool src_is_dir = !src_sub.empty();
                // bool dst_is_dir = !src_sub.empty();
                // assert(!src_is_dir and !dst_is_dir);
                // if (!src_is_dir and !dst_is_dir) {
                return RenameFileFile(src, dst);
                // }
                // else if (!src_is_dir and dst_is_dir) {
                //     // todo
                // }
                // else if (src_is_dir and !dst_is_dir) {
                //     // todo
                // }
                // else {
                //     assert(src_is_dir);
                //     assert(dst_is_dir);
                //     return RenameDirDir(src, dst);
                // }
        }

        void RenameDirDir(const std::string& src, const std::string& /*dst*/) {
                // todo
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                const auto& src_dir = str2dirs(src);
	
                for (const auto& f: meta_in_mem) {
                        File* fptr = get_file(f.first);
                        assert(fptr != nullptr);
                        assert(fptr->getPath() == f.first);

                        const auto& [dir, fil] = f.first;

                        if (dir.size() < src_dir.size()) {
                                continue;
                        }
	    
                        assert(dir.size() >= src_dir.size());
                        bool retry = false;
                        for (size_t i = 0; i < src_dir.size(); i++) {
                                if (dir[i] != src_dir[i]) {
                                        retry = true;
                                        break;
                                }
                        }
                        if (retry) {
                                continue;
                        }

                        if (dir.size() == src_dir.size()) {
                                //ret.insert(filename2str(fil));
                        }
                        else {
                                assert(dir.size() > src_dir.size());
                                //ret.insert(dir2str(dir[target_dirs.size()]));
                        }
                }
        }
    
        void RenameFileFile(const std::string& src, const std::string& dst) {
                if (src == dst) { return; }
                std::lock_guard<decltype(mtx_)> lock(mtx_);

                auto src_fno = fileno(src);
                [[maybe_unused]] const auto& [src_dir, src_filename] = src_fno;
                auto dst_fno = fileno(dst);
                [[maybe_unused]] const auto& [dst_dir, dst_filename] = dst_fno;
                if (src_fno == dst_fno) {
                        return;
                }

                try { // remove dst fistly
                        [[maybe_unused]] File* f_dst = get_file(dst_fno);
                        assert(!f_dst->is_open());
                        Remove(dst_fno);
                }
                catch (...) {}

                assert((dst_filename != src_filename) || (dst_dir != src_dir));

                auto src_itr = meta_in_mem.find(src_fno);
                if (src_itr == meta_in_mem.end()) {
                        throw FileNotFound("remove file file source " + src);
                }
                [[maybe_unused]] File* f_src = get_file(src_fno);
                assert(f_src != nullptr);
                assert(!f_src->is_unused());
                assert(f_src->getPath() == src_fno);

                auto phys_block = src_itr->second;
                meta_in_mem.erase(src_fno);

                meta_in_mem.emplace(dst_fno, phys_block);

                File* f = get_file(dst_fno);
                assert(f != nullptr);
	       
                *f = File(dst_dir, dst_filename.name_, dst_filename.suffix_, f->length_);
                assert(f->is_closed());
                assert(f->getPath() == dst_fno);
	
                set_dirty(f);

                check_free_blocks();
        }

        // uint64_t ModificationTime(const std::string& src) {
        //         abort();
        // }


        // bool SupportFileType(const std::string& path) const {
        //     try {
        //         fileno(path);
        //         return true;
        //     }
        //     catch (FileNotFound&) {
        //         return false;
        //     }
        //     catch (DirNotFound&) {
        //         return false;
        //     }
        // }

        uint64_t MaxFileSize() const {
                return block_length_ * (INDIRECT_BLOCKS_MAX_NUM + 1);
        }

        uint64_t HeadLBA(const FdpFsFd& fd) const {
                return fd.head_;
        }

        uint64_t FreeBlocks() const {
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                return free_blocks.size();
        }

        uint64_t FreeSpace() const {
                std::lock_guard<decltype(mtx_)> lock(mtx_);
                return free_blocks.size() * block_length_;
        }
};


#include <cassert>
#include <sys/uio.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <execinfo.h>
#include <string>
#include <vector>
#include <memory>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <mutex>

static inline std::string get_backtrace() {
        std::vector<void*> buffer(1024);
        std::string ret;
        int n = backtrace(&buffer[0], buffer.size());

        std::unique_ptr<char*, decltype(free)*> strings(backtrace_symbols(&buffer[0], n), free);
        for (int i = 0; i < n; i++) {
                ret += '\n';
                ret += strings.get()[i];
        }
        return ret;
}

template<typename T>
struct aligned_buffer {
private:
        void* ptr_;
        std::size_t size_;
public:
        aligned_buffer() : ptr_(nullptr), size_(0) {
        }
        aligned_buffer(std::size_t s) : size_(s) {
                ptr_ = aligned_alloc(4096, s);
                if (ptr_ == nullptr) {
                        throw std::bad_alloc();
                }
        }
        ~aligned_buffer() {
                free(ptr_);
        }
        T* data() const {
                return static_cast<T*>(ptr_);
        }
        std::size_t size() const {
                return size_;
        }
};

template<typename T>
struct sized_buffer {
private:
        void* ptr_;
        std::size_t size_;
public:
        sized_buffer() : ptr_(nullptr), size_(0) {}
        sized_buffer(void* ptr, std::size_t s) : ptr_(ptr), size_(s) {}
        ~sized_buffer() = default;
        T* data() const {
                return static_cast<T*>(ptr_);
        }
        std::size_t size() const {
                return size_;
        }
};

template<typename T>
static inline
ssize_t BufferedRead(FdpFs<T>& fs, typename FdpFs<T>::FdpFsFd& fd, char* scratch, size_t n, uint64_t offset) {

        assert(scratch != nullptr);

        if (offset >= fs.FileSize(fd)) {
                return 0; // no read
        }

        if (offset + n > fs.FileSize(fd)) {
                n = fs.FileSize(fd) - offset;
        }

        auto aligned_offset = round_down(offset, 4096UL);
        auto aligned_len = round_up(offset + n, 4096UL) - aligned_offset;

        aligned_buffer<char> buf(aligned_len);
        assert((buf.size() % 4096) == 0);
        // assert(buf.size() >= n);
        // assert(buf.size() <= n + 4096);

        auto head_skip = offset - aligned_offset;
        auto tail_skip = aligned_len - n - head_skip;

        assert((aligned_offset % 4096) == 0);
        assert((aligned_len % 4096) == 0);
        assert(aligned_len == (head_skip + n + tail_skip));
        assert(aligned_offset <= offset);
        assert(n <= aligned_len);

        //    4kb    offset
        //             v          n
        //             |<-------------------->|
        // |-------|-------|-------|-------|-------|-------|
        //         |                               |
        //         |<----------------------------->|
        //         ^         aligned_len
        //   aligned_offset
        //         |<->|                      |<-->|
        //       head_skip                  tail_skip
        
        size_t total_read = 0;
        size_t total_copy = 0;
        // std::cout << "offset " << offset << " aligned_offset " << aligned_offset << " n " << n << " aligned_len " << aligned_len << " head_skip " << head_skip << " tail_skip " << tail_skip << std::endl;
        while (total_read < aligned_len) {
                size_t k = fs.Read(fd, buf, aligned_offset + total_read);
      
                bool first_read = total_read == 0;
                bool last_read = (k < buf.size()) || ((total_read + k) == aligned_len);
                size_t copy_len = buf.size() - (first_read ? head_skip : 0) - (last_read ? tail_skip : 0);
                // std::cout << "total_copy " << total_copy << " copy_len " << copy_len << " total_read " << total_read << " last_read? " << last_read << " k " << k << std::endl;
                assert(buf.size() >= (first_read ? head_skip : 0) + (last_read ? tail_skip : 0)); // equal to assert(copy_len >= 0);
                assert(!last_read || (buf.size() - tail_skip) <= k);
                assert(buf.size() >= copy_len + (first_read ? head_skip : 0));
                assert(total_copy + copy_len <= n);
                assert(buf.size() > (first_read ? head_skip : 0));
                assert(total_copy < n);
                memcpy(scratch + total_copy, buf.data() + (first_read ? head_skip : 0), copy_len);
                
                total_copy += copy_len;
                total_read += k;
                
                if (last_read) {
                        break;
                }
        }
        // std::cout << std::endl;
        // std::cout << "offset " << offset << " aligned_offset " << aligned_offset << " n " << n << " aligned_len " << aligned_len << " head_skip " << head_skip << " tail_skip " << tail_skip << " total_read " << total_read << " total_copy " << total_copy << std::endl;
        assert(total_read <= aligned_len);
        assert(total_copy == n);

        return total_read;
}
