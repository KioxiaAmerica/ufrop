//  Copyright (c) 2024, Kioxia corporation.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "env/composite_env_wrapper.h"
#include "env/io_posix.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "port/lang.h"
#include "port/port.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/object_registry.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/compression_context_cache.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/thread_local.h"
#include "util/threadpool_imp.h"

#include <unistd.h>

#include <iostream>
#include <csignal>
#include "fdpfs.hpp"


template<typename T>
class array_view {
        const T& arr_;
        size_t off_, len_;
public:
        array_view(const T& arr, size_t off, size_t len) : arr_(arr), off_(off), len_(len) {
                assert(arr.size() >= off + len);
        }
        typename T::value_type const * data() const {
                return &arr_.at(off_);
        }
        size_t size() const {
                return len_;
        }
};

static void sig_handler(int signo, [[maybe_unused]] siginfo_t *info, [[maybe_unused]] void *context) {
        printf("%s %d\n", __FUNCTION__, signo);
        exit(0);
}

static void register_sighandler() {
        struct sigaction act = { 0 };
        act.sa_flags = SA_SIGINFO;
        act.sa_sigaction = &sig_handler;

        if (sigaction(SIGINT, &act, NULL) == -1) {
                perror("sigaction");
        }
        // if (sigaction(SIGTERM, &act, NULL) == -1) {
        //         perror("sigaction");
        // }
}


namespace ROCKSDB_NAMESPACE {

        template<typename T>
        class FdpWritableFile : public FSWritableFile {
        private:
                FdpFs<T>& fs_;
                typename FdpFs<T>::FdpFsFd fd_;
                std::string fname_;
                alignas(4096) std::array<char,1024*1024*2> buf;
                uint64_t buf_offset_;
                uint64_t synced_buf_offset_;
                //uint64_t file_size_;
        public:
                FdpWritableFile(FdpFs<T>& fs, typename FdpFs<T>::FdpFsFd&& fd, const std::string& fname) : fs_(fs), fd_(std::move(fd)), fname_(fname), buf_offset_(0), synced_buf_offset_(0) {
                        // std::cout << __FUNCTION__ << " " << fname << std::endl;                        
                        std::fill(buf.begin(), buf.end(), 0);
                }
                ~FdpWritableFile() {
                        Sync();
                        if (!fs_.Closed(fd_)) {
                                fs_.Close(fd_);
                        }
                        // if (fname_.substr(fname_.size() - 3) == "LOG") {
                        // std::cout << __FUNCTION__ << " " << fname_ << std::endl;
                        // }
                }
                IOStatus Truncate(uint64_t size, const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) override {
                        // if (fname_.substr(fname_.size() - 3) == "LOG") {
                        //         std::cout << fname_ << " " << fs_.FileSize(fd_) << " trunc " << size << std::endl;
                        // }

                        try {
                                fs_.Truncate(fd_, size);
                        } catch (typename FdpFs<T>::FileNotFound&) {
                                // pass because trunc after delete
                        }
                        return IOStatus::OK();
                }
                IOStatus Close(const IOOptions& opts,
                               IODebugContext* dbg) override {
                        Sync(opts, dbg);
                        fs_.Close(fd_);
                        return IOStatus::OK();
                }

                IOStatus Append(const Slice& data, const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {      
                        assert(buf_offset_ < buf.size());
                        assert(synced_buf_offset_ <= buf_offset_);
                        CHECK_FD(fd_);
                        // if (fname_.substr(fname_.size() - 3) == "LOG") {
                        //         std::cout << fname_ << " " << fs_.FileSize(fd_) << " append " << data.size() << std::endl;
                        // }
                        if (fs_.FileSize(fd_) + buf_offset_ + data.size() > fs_.MaxFileSize(fd_)) {
                                std::cout << fname_ << " append nospace size " << fs_.FileSize(fd_) << " buf off " << buf_offset_ << " append buf " << data.size() << " in fs_fdp" << std::endl;
                                return IOStatus::NoSpace(fname_ + " append");
                        }

                        for (size_t written = 0; written < data.size();) {
                                assert(buf_offset_ <= buf.size());
                                assert(written <= data.size());

                                auto len = std::min(buf.size() - buf_offset_, data.size() - written);
                                memcpy(&buf.at(buf_offset_), data.data() + written, len);
                                buf_offset_ += len;
                                written += len;
                                //file_size_ += len;

                                if (buf_offset_ == buf.size()) {
                                        auto start = round_down<uint64_t>(synced_buf_offset_, 4096);
                                        auto goal = round_up<uint64_t>(buf_offset_, 4096);
                                        auto sub_array = array_view(buf, start, goal - start);
                                        assert(goal <= buf.size());

                                        assert(sub_array.size() <= buf.size());
                                        try {
                                                fs_.Append(fd_, sub_array);
                                        }
                                        catch (...) {
                                                auto ret = IOStatus::NoSpace();
                                                // ret.SetRetryable(true);
                                                return ret;
                                        }
                                        buf_offset_ = 0;
                                        synced_buf_offset_ = 0;
                                        std::fill(buf.begin(), buf.end(), 0);

                                        //assert(file_size_ == fs_.FileSize(fd_));
                                }
                                assert(buf_offset_ < buf.size());
                        }
                        assert(buf_offset_ < buf.size());
                        return IOStatus::OK();
                }
                IOStatus Append(const Slice& data, const IOOptions& opts,
                                const DataVerificationInfo& /* verification_info */,
                                IODebugContext* dbg) override {
                        return Append(data, opts, dbg);
                }
                IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                          const IOOptions& /*opts*/,
                                          IODebugContext* /*dbg*/) override {
                        
                        try {
                                fs_.PositionedAppend(fd_, data, offset);
                        }
                        catch (...) {
                                std::cout << "positioned append fail " << fname_ << " file size " << fs_.FileSize(fd_)
                                          << " data size " << data.size() << " offset " << offset << std::endl;
                                auto ret = IOStatus::NoSpace();
                                // ret.SetRetryable(true);
                                return ret;
                        }
                        // std::cout << fname_ << " size " << fs_.FileSize(fd_) << std::endl;
                        return IOStatus::OK();
                }
                IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                          const IOOptions& opts,
                                          const DataVerificationInfo& /* verification_info */,
                                          IODebugContext* dbg) override {
                        return PositionedAppend(data, offset, opts, dbg);
                }

                // Flush write buffer to the OS
                IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override {
                        Sync(opts, dbg);
                        return IOStatus::OK();
                }

                // Sync the data in OS to the device
        private:
                IOStatus Sync() {
                        assert(buf_offset_ < buf.size());
                        assert(synced_buf_offset_ <= buf_offset_);

                        if (buf_offset_ == synced_buf_offset_) {
                                // no unsynced buffer
                                return IOStatus::OK();
                        }
                        CHECK_FD(fd_);

                        auto start = round_down<uint64_t>(synced_buf_offset_, 4096);
                        auto goal = round_up<uint64_t>(buf_offset_, 4096);
                        auto sub_array = array_view(buf, start, goal - start);

                        fs_.Append(fd_, sub_array, buf_offset_ - start);
                        synced_buf_offset_ = buf_offset_;

                        //assert(file_size_ == fs_.FileSize(fd_));

                        fs_.Sync();
                        return IOStatus::OK();

                }
        public:
                IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
                        return Sync();
                }
                IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override {
                        return Sync(opts, dbg);
                }
                bool IsSyncThreadSafe() const override {
                        return true;
                }

                bool use_direct_io() const override { return true; }
                void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
                        std::cout << "WritableFile " << fname_ << " hint "
                                  << hint << std::endl;
                        PlacementId pid(hint);
                        fd_.set_placement_id(pid);
                }
                uint64_t GetFileSize(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
                        //assert(file_size_ == fs_.FileSize(fd_));
                        return fs_.FileSize(fd_);
                }

                IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
                        return IOStatus::OK();
                }
                size_t GetRequiredBufferAlignment() const override {
                        return 4096;
                }
        };

        template<typename T>
        class FdpSequentialFile : public FSSequentialFile {
        private:
                FdpFs<T>& fs_;
                typename FdpFs<T>::FdpFsFd fd_;
                alignas(4096) std::array<char, 1024*1024> buf; // 1MB read ahead
                uint64_t buf_offset_;
                uint64_t cur_;
                std::string fname_;
        public:
                FdpSequentialFile(FdpFs<T>& fs, typename FdpFs<T>::FdpFsFd&& f, const std::string& fname) : fs_(fs), fd_(std::move(f)), buf_offset_(0), cur_(0), fname_(fname) {
                        // std::cout << __FUNCTION__ << " " << fname << std::endl;
                }
                virtual ~FdpSequentialFile() { fs_.Close(fd_); }

                IOStatus Read(size_t n, const IOOptions& /*opts*/, Slice* result, char* scratch,
                              IODebugContext* /*dbg*/) override {
                        assert(scratch != nullptr);
    
                        CHECK_FD(fd_);


                        uint64_t file_size = fs_.FileSize(fd_);
                        assert(cur_ <= file_size);

                        uint64_t total_read = 0;
                        for (size_t i = 0; i < std::min(n, file_size - cur_); i++, buf_offset_++) {
                                if (buf_offset_ == buf.size()) {
                                        buf_offset_ = 0;
                                }

                                if (buf_offset_  == 0) {
                                        if (fs_.Eof(fd_)) {
                                                break;
                                        }
                                        fs_.Read(fd_, buf);
                                }
                                scratch[i] = buf.at(buf_offset_);
                                total_read++;
                        }
                        cur_ += total_read;
                        assert(cur_ <= file_size);
                        *result = Slice(scratch, total_read);
                        return IOStatus::OK();
                }

                IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& opts,
                                        Slice* result, char* scratch,
                                        IODebugContext* dbg) override {
                        uint64_t cur_save = fs_.Seek(fd_, 0, FdpFs<T>::SeekWhence::CUR);
                        fs_.Seek(fd_, offset, FdpFs<T>::SeekWhence::SET);
                        auto ret = Read(n, opts, result, scratch, dbg);
                        fs_.Seek(fd_, cur_save, FdpFs<T>::SeekWhence::SET);
                        return ret;
                }
                IOStatus Skip(uint64_t n) override {
                        fs_.Seek(fd_, n, FdpFs<T>::SeekWhence::CUR);
                        return IOStatus::OK();
                }
                IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
                        return IOStatus::OK();
                }
                bool use_direct_io() const override { return true; }
                size_t GetRequiredBufferAlignment() const override {
                        return 4096;
                }

        };
  
        template<typename T>
        class FdpRandomAccessFile : public FSRandomAccessFile {
                FdpFs<T>& fs_;
                mutable typename FdpFs<T>::FdpFsFd fd_;
                // char* cache_;
        protected:
                std::string fname_;
        public:
                FdpRandomAccessFile(FdpFs<T>& fs, typename FdpFs<T>::FdpFsFd&& fd, const std::string fname) : fs_(fs), fd_(std::move(fd)), fname_(fname) {
                        // std::cout << __FUNCTION__ << " " << fname << std::endl;
                }
                
                virtual ~FdpRandomAccessFile() {
                        fs_.Close(fd_);
                        // free(cache_);
                        // if (fname_.substr(fname_.size() - 3) == "LOG") {
                        //         std::cout << __FUNCTION__ << " " << fname_ << std::endl;
                        // }
                }
                
                IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*opts*/, Slice* result,
                              char* scratch, IODebugContext* /*dbg*/) const override {
                        if (offset > fs_.FileSize(fd_)) {
                                *result = Slice(scratch, 0);
                                return IOStatus::OK();
                        }

                        if (offset + n > fs_.FileSize(fd_)) {
                                n = fs_.FileSize(fd_) - offset;
                        }

                        bool direct = (((offset % 4096UL) == 0) and
                                       ((n % 4096UL) == 0) and
                                       ((reinterpret_cast<uintptr_t>(scratch) % 4096UL) == 0));

                        auto k = direct ? ReadDirect(offset, n, scratch) : ReadBuffered(offset, n, scratch);
                        if (k < 0) {
                                return IOStatus::IOError("Read " + fname_);
                        }
                        assert(k >= 0);
                        assert(static_cast<size_t>(k) <= n);
                        *result = Slice(scratch, k);
                        return IOStatus::OK();
                }

                ssize_t ReadDirect(uint64_t offset, size_t n, char* scratch) const {

                        assert(offset <= fs_.FileSize(fd_));
                        assert(offset + n <= fs_.FileSize(fd_));

                        assert(scratch != nullptr);
                        assert((offset % 4096UL) == 0);
                        assert((n % 4096UL) == 0);
                        assert((reinterpret_cast<uintptr_t>(scratch) % 4096UL) == 0);
                        
                        //    4kb  offset
                        //         v      n = scratch
                        //         |<--------------------->|
                        // |-------|-------|-------|-------|-------|-------|
                        //         |                       |
                        size_t total_read = 0;
                        //std::cout << __FUNCTION__ << " " << fname_ << " offset " << offset << " n " << n << std::endl;
                        while (total_read < n) {
                                sized_buffer<char> buf = sized_buffer<char>(scratch + total_read, n - total_read);
                                size_t k = fs_.Read(fd_, buf, offset + total_read);
                                if (k <= 0) {
                                        break;
                                }
                                assert(k > 0);
                                total_read += k;
                        }
                        assert(total_read == n);
                        return n;
                }

                
                ssize_t ReadBuffered(uint64_t offset, size_t n, char* scratch) const {

                        assert(scratch != nullptr);

                        if (offset > fs_.FileSize(fd_)) {
                                return 0;
                        }

                        if (offset + n > fs_.FileSize(fd_)) {
                                n = fs_.FileSize(fd_) - offset;
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
                        //std::cout << __FUNCTION__ << " " << fname_ << " offset " << offset << " aligned_offset " << aligned_offset << " n " << n << " aligned_len " << aligned_len << " head_skip " << head_skip << " tail_skip " << tail_skip << std::endl;
                        while (total_read < aligned_len) {
                                size_t k = fs_.Read(fd_, buf, aligned_offset + total_read);
      
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

                        assert(total_read <= aligned_len);
                        assert(total_copy == n);
                        
                        return n;
                }


                IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/, const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) override {
                        return IOStatus::NotSupported();
                }


                size_t GetUniqueId(char* id, size_t max_size) const override {
                        std::cout << __FUNCTION__ << " " << max_size << std::endl;
                        if (max_size < kMaxVarint64Length * 2) {
                                return 0;
                        }

                        uint64_t uversion = 0x123fbeef;
                        
                        char* rid = id;
                        rid = EncodeVarint64(rid, fd_.GetInode());
                        rid = EncodeVarint64(rid, uversion);
                        assert(rid >= id);
                        return static_cast<size_t>(rid - id);
                }

                void Hint(AccessPattern /*pattern*/) override {
                        // std::cout << "RandomAccess " << fname_ << " pattern " << pattern << std::endl;
                }
                IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
                        return IOStatus::OK();
                }
                bool use_direct_io() const override { return true; }
                size_t GetRequiredBufferAlignment() const override {
                        return 4096;
                }

        };

        template<typename T>
        class FdpMmapReadableFile : public FdpRandomAccessFile<T> {
                mutable std::map<std::pair<uint64_t, size_t>, char*> pool_;
                mutable std::mutex mtx_;
        public:
                FdpMmapReadableFile(FdpFs<T>& fs, typename FdpFs<T>::FdpFsFd&& fd, const std::string fname) : FdpRandomAccessFile<T>(fs, std::move(fd), fname) {
                }
    
                virtual ~FdpMmapReadableFile() {

                        std::lock_guard<decltype(mtx_)> lock(mtx_);
                        for (const auto& p: pool_) {
                                free(p.second);
                        }
                }
                IOStatus Read(uint64_t offset, size_t n, const IOOptions& opts, Slice* result,
                              char* /*scratch*/, IODebugContext* dbg) const override {
                        std::lock_guard<decltype(mtx_)> lock(mtx_);
                        auto m = pool_.find(std::make_pair(offset, n));
                        char* ptr = nullptr;
                        if (m == pool_.end()) {
                                ptr = static_cast<char*>(malloc(n));
                                if (ptr == nullptr) {
                                        return IOStatus::IOError("buffer malloc fail");
                                }
                                pool_.emplace(std::make_pair(offset, n), ptr);
                        }
                        else {
                                ptr = m->second;
                        }
                        assert(ptr != nullptr);
                        return FdpRandomAccessFile<T>::Read(offset, n, opts, result, ptr, dbg);
                }
                bool use_direct_io() const override { return false; }
        };


        template<typename T>
        class FdpFileLock : public FileLock {
        public:
                std::string filename;
                typename FdpFs<T>::FdpFsFd fd_;
                FdpFileLock(const std::string& f, typename FdpFs<T>::FdpFsFd&& fd) : filename(f), fd_(std::move(fd)) {}
  
                void Clear() {
                        filename.clear();
                }

                ~FdpFileLock() override {

                }
        };

  
        class FdpFsDirectory : public FSDirectory {
        public:
                explicit FdpFsDirectory() {}
                ~FdpFsDirectory() {}
                IOStatus Fsync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
                        return IOStatus::OK();
                }

                IOStatus Close(const IOOptions&, IODebugContext* ) override {
                        return IOStatus::OK();
                }

                IOStatus FsyncWithDirOptions(
                                             const IOOptions&, IODebugContext*,
                                             const DirFsyncOptions& ) override {
                        return IOStatus::OK();
                }
        };

        template<typename T>
        class FdpFileSystem : public FileSystem {
        
        public:
                FdpFileSystem(const char* path) : fdpfs_(path) {
                        std::cout << __FUNCTION__ << " initialization with " << path << std::endl;
                        register_sighandler();
                }

                static const char* kClassName() { return "FdpFileSystem"; }
                const char* Name() const override { return kClassName(); }
                const char* NickName() const override { return kDefaultName(); }

                ~FdpFileSystem() override = default;
                bool IsInstanceOf(const std::string& name) const override {
                        if (name == "fdp" || name == "fdpring") {
                                return true;
                        } else {
                                return FileSystem::IsInstanceOf(name);
                        }
                }

                IOStatus NewSequentialFile(
                                           const std::string& fname, const FileOptions& /*options*/,
                                           std::unique_ptr<FSSequentialFile>* result,
                                           IODebugContext* /*dbg*/) override {
                        // std::cout << __FUNCTION__ << " " << fname << std::endl;                        
                        auto fd = fdpfs_.Open(fname, FdpFs<T>::OperationMode::ReadAppend, FdpFs<T>::CreateMode::CreateIfMissing, PlacementId::DATA_SEQUENTIAL);
                        result->reset(new FdpSequentialFile<T>(fdpfs_, std::move(fd), fname));
                        return IOStatus::OK();
                }

                IOStatus NewRandomAccessFile(const std::string& fname,
                                             const FileOptions& options,
                                             std::unique_ptr<FSRandomAccessFile>* result,
                                             IODebugContext* /*dbg*/) override {
                        try {
                                assert(!options.use_mmap_writes);
                                auto fd = fdpfs_.Open(fname, FdpFs<T>::OperationMode::ReadOnly, FdpFs<T>::CreateMode::ErrorIfMissing);
                                if (options.use_mmap_reads) {
                                        // std::cout << "NewRnadomMmapFile " << fname << std::endl;
                                        result->reset(new FdpMmapReadableFile<T>(fdpfs_, std::move(fd), fname));
                                } else {
                                        // std::cout << "NewRnadomAccessFile " << fname << std::endl;
                                        result->reset(new FdpRandomAccessFile<T>(fdpfs_, std::move(fd), fname));
                                }
                                return IOStatus::OK();
                        } catch (typename FdpFs<T>::TooManyOpenFiles& t) {
                                return IOStatus::IOError(fname);
                        } catch (...) {
                                return IOStatus::NotFound(fname);
                        }
                }
    
                virtual IOStatus OpenWritableFile(const std::string& fname,
                                                  const FileOptions& options, bool /*reopen*/,
                                                  std::unique_ptr<FSWritableFile>* result,
                                                  IODebugContext* /*dbg*/) {
                        // std::cout << __FUNCTION__ << " " << fname << std::endl;                        
                        if (options.use_mmap_writes) {
                                return IOStatus::InvalidArgument("WritableFile mmap writes");
                        }
                        // if (options.use_mmap_reads) {
                        // 	return IOStatus::InvalidArgument("WritableFile mmap reads");
                        // }
                        std::string new_fname = fname;
                        if (fname[0] != '/') {
                                new_fname = '/' + fname;
                        }
                        uint64_t free_blocks = fdpfs_.FreeBlocks();
                        std::cout << "Remaining free blocks " << free_blocks << std::endl;
                        try {
                                auto fd = fdpfs_.Open(new_fname, FdpFs<T>::OperationMode::WriteOnly, FdpFs<T>::CreateMode::CreateIfMissing, PlacementId::DATA_WRITABLE);
                                result->reset(new FdpWritableFile<T>(fdpfs_, std::move(fd), fname));
                                return IOStatus::OK();
                        }
                        catch (typename FdpFs<T>::TooManyOpenFiles&) {
                                return IOStatus::IOError("too many open files " + fname);
                        }
                        catch (...) {
                                return IOStatus::NoSpace();
                        }
                }

                IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                                         std::unique_ptr<FSWritableFile>* result,
                                         IODebugContext* dbg) override {
                        return OpenWritableFile(fname, options, false, result, dbg);
                }

                IOStatus ReopenWritableFile(const std::string& fname,
                                            const FileOptions& options,
                                            std::unique_ptr<FSWritableFile>* result,
                                            IODebugContext* dbg) override {
                        return OpenWritableFile(fname, options, true, result, dbg);
                }

                IOStatus ReuseWritableFile(
                                           const std::string& /*fname*/,
                                           const std::string& /*old_fname*/,
                                           const FileOptions& /*options*/,
                                           std::unique_ptr<FSWritableFile>* /*result*/,
                                           IODebugContext* /*dbg*/) override {
                        return IOStatus::NotSupported();
                }

                IOStatus NewRandomRWFile(
                                         const std::string& /*fname*/,
                                         const FileOptions& /*options*/,
                                         std::unique_ptr<FSRandomRWFile>* /*result*/,
                                         IODebugContext* /*dbg*/) override {
                        return IOStatus::NotSupported();
                }

                IOStatus NewMemoryMappedFileBuffer(
                                                   const std::string& /*fname*/,
                                                   std::unique_ptr<MemoryMappedFileBuffer>* /*result*/)
                        override {
                        return IOStatus::NotSupported();
                }

                IOStatus NewDirectory(const std::string& /*name*/,
                                      const IOOptions& /*opts*/,
                                      std::unique_ptr<FSDirectory>* result,
                                      IODebugContext* /*dbg*/) override {
                        result->reset(new FdpFsDirectory());
                        return IOStatus::OK();
                }

                IOStatus FileExists(const std::string& fname,
                                    const IOOptions& /*opts*/,
                                    IODebugContext* /*dbg*/) override {
                        //std::cout << __FUNCTION__ << " " << fname << std::endl;                        
                        if (fdpfs_.Exists(fname)) {
                                return IOStatus::OK();
                        }
                        else {
                                return IOStatus::NotFound();
                        }
                }

                IOStatus GetChildren(const std::string& dir,
                                     const IOOptions& /*opts*/,
                                     std::vector<std::string>* result,
                                     IODebugContext* /*dbg*/) override {
                        //std::cout << __FUNCTION__ << " " << dir << std::endl;
                        result->clear();
                        *result = fdpfs_.Readdir(dir);
                        return IOStatus::OK();
                }

                IOStatus DeleteFile(const std::string& fname,
                                    const IOOptions& /*opts*/,
                                    IODebugContext* /*dbg*/) override {
                        // std::cout << __FUNCTION__ << " " << fname << std::endl;
                        try {
                                fdpfs_.Remove(fname);
                        } catch (std::exception &e) {
                                return IOStatus::NotFound();
                        }
                        return IOStatus::OK();
                }

                IOStatus CreateDir(const std::string& /*name*/,
                                   const IOOptions& /*opts*/,
                                   IODebugContext* /*dbg*/) override {
                        return IOStatus::OK();
                }

                IOStatus CreateDirIfMissing(const std::string& /*name*/,
                                            const IOOptions& /*opts*/,
                                            IODebugContext* /*dbg*/) override {
                        return IOStatus::OK();
                }

                IOStatus DeleteDir(const std::string& name,
                                   const IOOptions& /*opts*/,
                                   IODebugContext* /*dbg*/) override {
                        //std::cout << __FUNCTION__ << " " << name << std::endl;
                        auto files = fdpfs_.Readdir(name);
                        for (const auto& f: files) {
                                try {
                                        fdpfs_.Remove(name + '/' + f);
                                }
                                catch (...) {}
                        }

                        return IOStatus::OK();
                }

                IOStatus GetFileSize(const std::string& fname,
                                     const IOOptions& /*opts*/, uint64_t* size,
                                     IODebugContext* /*dbg*/) override {
                        try {
                                * size = fdpfs_.FileSize(fname);
                                return IOStatus::OK();
                        }
                        catch(typename FdpFs<T>::FileNotFound&) {
                                return IOStatus::NotFound();
                        }
                }

                IOStatus GetFileModificationTime(
                                                 const std::string& /*fname*/, const IOOptions& /*opts*/,
                                                 uint64_t* /*file_mtime*/,
                                                 IODebugContext* /*dbg*/) override {
                        return IOStatus::NotSupported();
                }

                IOStatus RenameFile(const std::string& src,
                                    const std::string& target,
                                    const IOOptions& /*opts*/,
                                    IODebugContext* /*dbg*/) override {
                        try {
                                fdpfs_.Rename(src, target);
                        } catch (...) {
                        }
                        return IOStatus::OK();
                }

                IOStatus LinkFile(const std::string& /*src*/,
                                  const std::string& /*target*/,
                                  const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) override {
                        return IOStatus::NotSupported();
                }

                IOStatus NumFileLinks(const std::string& /*fname*/,
                                      const IOOptions& /*opts*/,
                                      uint64_t* count,
                                      IODebugContext* /*dbg*/) override {
                        *count = 1; // no suport for hard link
                        return IOStatus::OK();
                }

                IOStatus AreFilesSame(const std::string& first, const std::string& second,
                                      const IOOptions& /*opts*/, bool* res,
                                      IODebugContext* /*dbg*/) override {

                        if (!fdpfs_.Exists(first) or !fdpfs_.Exists(second)) {
                                return IOStatus::IOError("are file same");
                        }
                        *res = (first == second);
                        return IOStatus::OK();
                }

                IOStatus LockFile(const std::string& fname,
                                  const IOOptions& /*opts*/, FileLock** lock,
                                  IODebugContext* /*dbg*/) override {
                        if (fdpfs_.Exists(fname)) {
                                // return IOStatus::IOError("Lock file exists");
                                fdpfs_.Remove(fname);
                        }
                        auto fd = fdpfs_.Open(fname, FdpFs<T>::OperationMode::AppendOnly, FdpFs<T>::CreateMode::CreateIfMissing, PlacementId::DATA_EXTREME);
                        *lock = new FdpFileLock<T>(fname, std::move(fd));

                        return IOStatus::OK();
                }

                IOStatus UnlockFile(FileLock* lock, const IOOptions& /*opts*/,
                                    IODebugContext* /*dbg*/) override {
                        FdpFileLock<T>* l = static_cast<FdpFileLock<T>*>(lock);
                        fdpfs_.Close(l->fd_);
                        try {
                                fdpfs_.Remove(l->filename);
                        }
                        catch (typename FdpFs<T>::FileNotFound&) {
                        }
                        delete l;
                        return IOStatus::OK();
                }

                IOStatus GetAbsolutePath(const std::string& db_path,
                                         const IOOptions& /*opts*/,
                                         std::string* output_path,
                                         IODebugContext* /*dbg*/) override {
                        if (!db_path.empty() && db_path[0] == '/') {
                                *output_path = db_path;
                                return IOStatus::OK();
                        }
                        *output_path = "/";
                        return IOStatus::OK();
                }

                IOStatus GetTestDirectory(const IOOptions& /*opts*/,
                                          std::string* result,
                                          IODebugContext* /*dbg*/) override {
                        *result = "";
                        return IOStatus::OK();
                }

                IOStatus GetFreeSpace(const std::string& /*fname*/,
                                      const IOOptions& /*opts*/,
                                      uint64_t* free_space,
                                      IODebugContext* /*dbg*/) override {
                        *free_space = fdpfs_.FreeSpace();
                        return IOStatus::OK();
                }

                IOStatus IsDirectory(const std::string& /*path*/,
                                     const IOOptions& /*opts*/, bool* is_dir,
                                     IODebugContext* /*dbg*/) override {
                        *is_dir = false;
                        return IOStatus::NotSupported();
                }

        private:
                FdpFs<T> fdpfs_;

        };
        
        FactoryFunc<FileSystem> fdp_filesystem_reg =
                ObjectLibrary::Default()->AddFactory<FileSystem>(
                                                                 ObjectLibrary::PatternEntry("fdpblock").AddSeparator("://", false),
                                                                 [](const std::string& uri, std::unique_ptr<FileSystem>* f,
                                                                    std::string* /* errmsg */) {
                                                                         std::string pat = "fdpblock://";
                                                                         f->reset(new FdpFileSystem<int>(uri.substr(pat.size()).c_str()));
                                                                         return f->get();
                                                                 });
        FactoryFunc<FileSystem> fdpring_filesystem_reg =
                ObjectLibrary::Default()->AddFactory<FileSystem>(
                                                                 ObjectLibrary::PatternEntry("fdpring").AddSeparator("://", false),
                                                                 [](const std::string& uri, std::unique_ptr<FileSystem>* f,
                                                                    std::string* /* errmsg */) {
                                                                         std::string pat = "fdpring://";
                                                                         f->reset(new FdpFileSystem<RingContext>(uri.substr(pat.size()).c_str()));
                                                                         return f->get();
                                                                 });
}  // namespace ROCKSDB_NAMESPACE
