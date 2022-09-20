# Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

import datetime
from ceresdb_client import Builder, RpcContext, PointBuilder, ValueBuilder, WriteRequest, QueryRequest, Mode
import asyncio


def create_table(ctx):
    create_table_sql = 'CREATE TABLE IF NOT EXISTS demo ( \
        name string TAG, \
        value double, \
        t timestamp NOT NULL, \
        TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl=false)'

    req = QueryRequest(['demo'], create_table_sql)
    _resp = sync_query(client, ctx, req)
    print("Create table success!")


def drop_table(ctx):
    drop_table_sql = 'DROP TABLE demo'

    req = QueryRequest(['demo'], drop_table_sql)
    _resp = sync_query(client, ctx, req)
    print("Drop table success!")


async def async_query(cli, ctx, req):
    return await cli.query(ctx, req)


def sync_query(cli, ctx, req):
    event_loop = asyncio.get_event_loop()
    return event_loop.run_until_complete(async_query(cli, ctx, req))


def process_query_resp(resp):
    print(f"Raw resp is:\n{resp}\n")

    print(f"Rows in the resp:")
    for row_idx in range(0, resp.row_num()):
        row_tokens = []
        schema = resp.schema()
        row = resp.get_row(row_idx)
        for col_idx in range(0, schema.num_cols()):
            name = schema.get_column_schema(col_idx).name()
            val = row.get_column_value(col_idx)
            row_tokens.append(f"{name}:{val}")
        print(f"row#{col_idx}: {','.join(row_tokens)}")


async def async_write(cli, ctx, req):
    return await cli.write(ctx, req)


def sync_write(cli, ctx, req):
    event_loop = asyncio.get_event_loop()
    return event_loop.run_until_complete(async_write(cli, ctx, req))


def process_write_resp(resp):
    print("success:{}, failed:{}".format(resp.get_success(), resp.get_failed()))


if __name__ == "__main__":
    client = Builder("127.0.0.1:8831", Mode.Standalone).build()
    ctx = RpcContext("public", "")

    print("------------------------------------------------------------------")
    print("### create table:")
    create_table(ctx)
    print("------------------------------------------------------------------")

    print("### write:")
    point_builder = PointBuilder()
    point_builder.metric('demo')
    point_builder.timestamp(int(round(datetime.datetime.now().timestamp())))
    point_builder.tag("name", ValueBuilder().with_str("test_tag1"))
    point_builder.field("value", ValueBuilder().with_double(0.4242))
    point = point_builder.build()
    write_request = WriteRequest()
    write_request.add_point(point)
    resp = sync_write(client, ctx, write_request)
    process_write_resp(resp)
    print("------------------------------------------------------------------")

    print("### read:")
    req = QueryRequest(['demo'], 'select * from demo')
    resp = sync_query(client, ctx, req)
    process_query_resp(resp)
    print("------------------------------------------------------------------")

    print("### drop table:")
    drop_table(ctx)
    print("------------------------------------------------------------------")
