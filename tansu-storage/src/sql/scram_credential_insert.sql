-- Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and

insert into scram_credential
(username, mechanism, salt, iterations, stored_key, server_key)
values
($1, $2, $3, $4, $5, $6)
on conflict (username, mechanism)
do update set
salt = excluded.salt,
iterations = excluded.iterations,
stored_key = excluded.stored_key,
server_key = excluded.server_key,
last_updated = excluded.last_updated
