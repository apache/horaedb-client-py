import enum
from typing import Any, List, Optional

# models


class SqlQueryRequest:
    def __init__(self, tables: List[str], sql: str): ...


class SqlQueryResponse:
    def row_num(self) -> int: ...
    def get_row(self, idx: int) -> Optional[Row]: ...
    @property
    def affected_rows(self) -> int: ...


class DataType(enum.IntEnum):
    Null = 0
    Timestamp = 1
    Double = 2
    Float = 3
    Varbinary = 4
    String = 5
    UInt64 = 6
    UInt32 = 7
    UInt16 = 8
    UInt8 = 9
    Int64 = 10
    Int32 = 11
    Int16 = 12
    Int8 = 13
    Boolean = 14


class Column:
    def value(self) -> Any: ...
    def data_type(self) -> DataType: ...
    def name(self) -> str: ...


class Row:
    def column_by_idx(self, idx: int) -> Any: ...
    def column_by_name(self, name: str) -> Any: ...
    def num_cols(self) -> int: ...


class Value:
    pass


class ValueBuilder:
    def __init__(self): ...
    def null(self) -> Value: ...
    def timestamp(self, val: int) -> Value: ...
    def varbinary(self, val: bytes) -> Value: ...
    def string(self, val: str) -> Value: ...
    def double(self, val: float) -> Value: ...
    def float(self, val: float) -> Value: ...
    def uint64(self, val: int) -> Value: ...
    def uint32(self, val: int) -> Value: ...
    def uint16(self, val: int) -> Value: ...
    def int64(self, val: int) -> Value: ...
    def int32(self, val: int) -> Value: ...
    def int16(self, val: int) -> Value: ...
    def uint8(self, val: int) -> Value: ...
    def bool(self, val: bool) -> Value: ...


class Point:
    pass


class PointBuilder:
    def __init__(self, table: str) -> PointBuilder: ...
    def table(self, table: str) -> PointBuilder: ...
    def timestamp(self, timestamp_ms: int) -> PointBuilder: ...
    def tag(self, name: str, val: Value) -> PointBuilder: ...
    def field(self, name: str, val: Value) -> PointBuilder: ...
    def build(self) -> Point: ...


class WriteRequest:
    def __init__(self): ...
    def add_point(self, point: Point): ...
    def add_points(self, point: List[Point]): ...


class WriteResponse:
    def get_success(self) -> int: ...
    def get_failed(self) -> int: ...

# client


class Client:
    def __init__(self, endpoint: str): ...

    async def query(self, ctx: RpcContext,
                    req: SqlQueryRequest) -> SqlQueryResponse: ...
    async def write(self, ctx: RpcContext,
                    req: WriteRequest) -> WriteResponse: ...


class RpcConfig:
    def __init__(self): ...
    thread_num: int
    max_send_msg_len: int
    max_recv_msg_len: int
    keepalive_time_ms: int
    keepalive_timeout_ms: int
    default_write_timeout_ms: int
    default_sql_query_timeout_ms: int
    connect_timeout_ms: int


class RpcContext:
    def __init__(self): ...
    timeout_ms: int
    database: str


class Builder:
    def __init__(self, endpoint: str): ...
    def rpc_config(self, conf: RpcConfig) -> Builder: ...
    def default_database(self, db: str) -> Builder: ...
    def build(self) -> Client: ...
