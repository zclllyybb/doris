// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/memory/cache_manager.h"

#include "runtime/memory/cache_policy.h"
#include "util/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"

int64_t CacheManager::for_each_cache_prune_stale_wrap(
        std::function<void(CachePolicy* cache_policy)> func, RuntimeProfile* profile) {
    int64_t freed_size = 0;
    std::lock_guard<std::mutex> l(_caches_lock);
    for (const auto& pair : _caches) {
        auto* cache_policy = pair.second;
        if (!cache_policy->enable_prune()) {
            continue;
        }
        func(cache_policy);
        freed_size += cache_policy->profile()->get_counter("FreedMemory")->value();
        if (cache_policy->profile()->get_counter("FreedMemory")->value() != 0 && profile) {
            profile->add_child(cache_policy->profile(), true, nullptr);
        }
    }
    return freed_size;
}

int64_t CacheManager::for_each_cache_prune_stale(RuntimeProfile* profile) {
    if (need_prune(&_last_prune_stale_timestamp, "stale")) {
        return for_each_cache_prune_stale_wrap(
                [](CachePolicy* cache_policy) { cache_policy->prune_stale(); }, profile);
    }
    return 0;
}

int64_t CacheManager::for_each_cache_prune_all(RuntimeProfile* profile, bool force) {
    if (force || need_prune(&_last_prune_all_timestamp, "all")) {
        return for_each_cache_prune_stale_wrap(
                [force](CachePolicy* cache_policy) { cache_policy->prune_all(force); }, profile);
    }
    return 0;
}

int64_t CacheManager::cache_prune_all(CachePolicy::CacheType type, bool force) {
    std::lock_guard<std::mutex> l(_caches_lock);
    auto* cache_policy = _caches[type];
    cache_policy->prune_all(force);
    return cache_policy->profile()->get_counter("FreedMemory")->value();
}

int64_t CacheManager::for_each_cache_refresh_capacity(double adjust_weighted,
                                                      RuntimeProfile* profile) {
    int64_t freed_size = 0;
    std::lock_guard<std::mutex> l(_caches_lock);
    for (const auto& pair : _caches) {
        auto* cache_policy = pair.second;
        if (!cache_policy->enable_prune()) {
            continue;
        }
        cache_policy->adjust_capacity_weighted(adjust_weighted);
        freed_size += cache_policy->profile()->get_counter("FreedMemory")->value();
        if (cache_policy->profile()->get_counter("FreedMemory")->value() != 0 && profile) {
            profile->add_child(cache_policy->profile(), true, nullptr);
        }
    }
    return freed_size;
}

void CacheManager::for_each_cache_reset_initial_capacity(double adjust_weighted) {
    std::lock_guard<std::mutex> l(_caches_lock);
    for (const auto& pair : _caches) {
        pair.second->reset_initial_capacity(adjust_weighted);
    }
}

#include "common/compile_check_end.h"
} // namespace doris
