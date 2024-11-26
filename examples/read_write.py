# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import asyncio
import datetime
from horaedb_client import Builder, RpcContext, PointBuilder, ValueBuilder, WriteRequest, SqlQueryRequest, Mode, RpcConfig, Authorization


def create_table(ctx):
    create_table_sql = 'CREATE TABLE IF NOT EXISTS demo ( \
        name string TAG, \
        value double, \
        t timestamp NOT NULL, \
        TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl=false)'

    req = SqlQueryRequest(['demo'], create_table_sql)
    _resp = sync_query(client, ctx, req)
    print("Create table success!")


def drop_table(ctx):
    drop_table_sql = 'DROP TABLE demo'

    req = SqlQueryRequest(['demo'], drop_table_sql)
    _resp = sync_query(client, ctx, req)
    print("Drop table success!")


async def async_query(cli, ctx, req):
    return await cli.sql_query(ctx, req)


def sync_query(cli, ctx, req):
    event_loop = asyncio.get_event_loop()
    return event_loop.run_until_complete(async_query(cli, ctx, req))


def process_query_resp(resp):
    print(f"Raw resp is:\n{resp}\n")

    print(f"Access row by index in the resp:")
    for row_idx in range(0, resp.num_rows()):
        row_tokens = []
        row = resp.row_by_idx(row_idx)
        for col_idx in range(0, row.num_cols()):
            col = row.column_by_idx(col_idx)
            row_tokens.append(f"{col.name()}:{col.value()}#{col.data_type()}")
        print(f"row#{row_idx}: {','.join(row_tokens)}")

    print(f"Access row by iter in the resp:")
    for row in resp.iter_rows():
        row_tokens = []
        for col in row.iter_columns():
            row_tokens.append(f"{col.name()}:{col.value()}#{col.data_type()}")
        print(f"row: {','.join(row_tokens)}")


async def async_write(cli, ctx, req):
    return await cli.write(ctx, req)


def sync_write(cli, ctx, req):
    event_loop = asyncio.get_event_loop()
    return event_loop.run_until_complete(async_write(cli, ctx, req))


def process_write_resp(resp):
    print("success:{}, failed:{}".format(
        resp.get_success(), resp.get_failed()))


if __name__ == "__main__":
    rpc_config = RpcConfig()
    rpc_config.thread_num = 1
    rpc_config.default_write_timeout_ms = 1000
    builder = Builder("127.0.0.1:8831", Mode.Direct)
    builder.set_rpc_config(rpc_config)
    builder.set_default_database("public")
    # Required when server enable auth
    builder.set_authorization(Authorization("test", "test"))
    client = builder.build()

    ctx = RpcContext()
    ctx.timeout_ms = 1000
    ctx.database = "public"

    print("------------------------------------------------------------------")
    print("### create table:")
    create_table(ctx)
    print("------------------------------------------------------------------")

    print("### write:")
    point_builder = PointBuilder('demo')
    point_builder.set_timestamp(
        int(round(datetime.datetime.now().timestamp())) * 1000)
    point_builder.set_tag("name", ValueBuilder().string("test_tag1"))
    point_builder.set_field("value", ValueBuilder().double(0.4242))
    point = point_builder.build()

    write_request = WriteRequest()
    write_request.add_point(point)
    resp = sync_write(client, ctx, write_request)
    process_write_resp(resp)
    print("------------------------------------------------------------------")

    print("### read:")
    req = SqlQueryRequest(['demo'], 'select * from demo')
    resp = sync_query(client, ctx, req)
    process_query_resp(resp)
    print("------------------------------------------------------------------")

    print("### drop table:")
    drop_table(ctx)
    print("------------------------------------------------------------------")
