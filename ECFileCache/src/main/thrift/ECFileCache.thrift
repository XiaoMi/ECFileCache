// Copyright 2016 Xiaomi, Inc.
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
namespace java com.xiaomi.filecache.thrift

struct FileCacheKey {
    1:required string uuid; // uuid, indicate an uniq session
    2:required byte version; // EC strategy version
    3:required i16 deviceClusterSize; // record current redis cluster 'size'
    4:required i16 deviceOffset; // use devices from 'deviceOffset' to '(k+m) % size' in redis cluster
    5:optional i64 fileSize;
}
