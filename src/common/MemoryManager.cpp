/* Base class for process to swapping memory to disk when low
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

OpenLogReplicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

OpenLogReplicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with OpenLogReplicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Ctx.h"
#include "MemoryManager.h"
#include "exception/RuntimeException.h"

namespace OpenLogReplicator {
    MemoryManager::MemoryManager(Ctx* newCtx, const std::string& newAlias, const char* newSwapPath) :
            Thread(newCtx, newAlias),
            swapPath(newSwapPath) {
    }

    MemoryManager::~MemoryManager() {
        cleanup();
    }

    void MemoryManager::wakeUp() {
        std::unique_lock<std::mutex> lck(ctx->swapMtx);
        ctx->chunksMemoryManager.notify_all();
    }

    void MemoryManager::run() {
        //if (unlikely(ctx->trace & Ctx::TRACE_THREADS)) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            //ctx->logTrace(Ctx::TRACE_THREADS,
            ctx->info(0, "memory manager (" + ss.str() + ") start");
        //}

        while (!ctx->hardShutdown) {
            cleanOldTransactions();

            if (ctx->softShutdown && ctx->replicatorFinished)
                break;

            {
                std::unique_lock<std::mutex> lck(ctx->swapMtx);
                ctx->chunksMemoryManager.wait_for(lck, std::chrono::milliseconds(100));
            }
        }

        //if (unlikely(ctx->trace & Ctx::TRACE_THREADS)) {
            //std::ostringstream ss;
            ss.clear();
            ss << std::this_thread::get_id();
            //ctx->logTrace(Ctx::TRACE_THREADS,
            ctx->info(0, "memory manager (" + ss.str() + ") stop");
        //}
    }

    void MemoryManager::initialize() {
        cleanup();
    }

    void MemoryManager::cleanOldTransactions() {
        while (true) {
            typeXid xid;
            SwapChunk* sc;
            {
                std::unique_lock<std::mutex> lck(ctx->swapMtx);
                if (ctx->commitedXids.empty())
                    return;

                xid = ctx->commitedXids.back();
                ctx->commitedXids.pop_back();
                auto it = ctx->swapChunks.find(xid);
                if (it == ctx->swapChunks.end())
                    continue;
                sc = it->second;
                ctx->swapChunks.erase(it);
            }
            delete sc;

            struct stat fileStat;
            std::string fileName(swapPath + "/" + xid.toString() + ".swap");
            if (stat(fileName.c_str(), &fileStat) == 0) {
                if (unlink(fileName.c_str()) != 0)
                    ctx->error(10010, "file: " + fileName + " - delete returned: " + strerror(errno));
            }
        }
    }

    void MemoryManager::cleanup() {
        DIR* dir;
        if ((dir = opendir(swapPath.c_str())) == nullptr)
            throw RuntimeException(10012, "directory: " + swapPath + " - can't read");

        struct dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;

            struct stat fileStat;
            std::string fileName(ent->d_name);

            std::string fullName(swapPath + "/" + ent->d_name);
            if (stat(fullName.c_str(), &fileStat) != 0) {
                ctx->warning(10003, "file: " + fileName + " - get metadata returned: " + strerror(errno));
                continue;
            }

            if (S_ISDIR(fileStat.st_mode))
                continue;

            std::string suffix(".swap");
            if (fileName.length() < suffix.length() || fileName.substr(fileName.length() - suffix.length(), fileName.length()) != suffix)
                continue;

            std::string fileBase(fileName.substr(0, fileName.length() - suffix.length()));
            ctx->warning(10072, "deleting old swap file from previous execution: " + fileBase);
            if (unlink(fileBase.c_str()) != 0)
                throw RuntimeException(10010, "file: " + fileBase + " - delete returned: " + strerror(errno));
        }
        closedir(dir);
    }
}
