import random

import pandas as pd
import pytest

import arctic
from arctic import Arctic
from arctic._config import FwPointersCfg


def gen_dataframe_random(cols, rows):
    c = {}
    for col in range(cols):
        c[str(col)] = [round(random.uniform(-10000.0, 10000.0), 1) for r in range(rows)]
    index = [range(rows)]

    return pd.DataFrame(data=c, index=index)


def gen_series_random(rows):
    col = [round(random.uniform(-10000.0, 10000.0), 1) for r in range(rows)]
    return pd.Series(data=col, index=list(range(rows)))


def gen_dataframe_compressible(cols, rows):
    row = [round(random.uniform(-100.0, 100.0), 1) for r in range(cols)]
    data = [row] * rows
    index = [range(rows)]

    return pd.DataFrame(data=data, index=index)


def gen_series_compressible(rows):
    d = round(random.uniform(-100.0, 100.0), 1)
    data = [d] * rows

    index = [range(rows)]

    return pd.Series(data=data, index=index)


# TEST_SIZES = [1000, 10000, 100000, 1000000]
TEST_SIZES = [1000]
df_random = [gen_dataframe_random(5, rows) for rows in TEST_SIZES]
s_random = [gen_series_random(5 * rows) for rows in TEST_SIZES]
df_compress = [gen_dataframe_compressible(10, rows) for rows in TEST_SIZES]
s_compress = [gen_series_compressible(rows) for rows in TEST_SIZES]

idx = random.randint(0, len(TEST_SIZES) - 1)


class FwPointersCtx:
    def __init__(self, value_to_test, do_reconcile=True):
        self.value_to_test = value_to_test
        self.do_reconcile = do_reconcile

    def __enter__(self):
        self.orig_value = arctic.store._ndarray_store.ARCTIC_FORWARD_POINTERS_CFG
        arctic.store._ndarray_store.ARCTIC_FORWARD_POINTERS_CFG = self.value_to_test

        self.reconcile_orig_value = arctic.store._ndarray_store.ARCTIC_FORWARD_POINTERS_RECONCILE
        arctic.store._ndarray_store.ARCTIC_FORWARD_POINTERS_RECONCILE = self.do_reconcile

    def __exit__(self, *args):
        arctic.store._ndarray_store.ARCTIC_FORWARD_POINTERS_CFG = self.orig_value
        arctic.store._ndarray_store.ARCTIC_FORWARD_POINTERS_RECONCILE = self.reconcile_orig_value


def write_data():
    store = Arctic("127.0.0.1")
    # store.delete_library('test.lib')
    store.initialize_library('test.lib')
    lib = store['test.lib']
    lib.write('df_bench_random', df_random[idx])

    lib.write('series_bench_random', s_random[idx])

    lib.write('df_bench_compressible', df_compress[idx])

    lib.write('series_bench_compressible', s_compress[idx])


def read_data():
    store = Arctic("127.0.0.1")
    # store.delete_library('test.lib')
    store.initialize_library('test.lib')
    lib = store['test.lib']
    lib.write('test_df', df_random[idx])
    lib.read('test_df')


def append_data():
    store = Arctic("127.0.0.1")
    # store.delete_library('test.lib')
    store.initialize_library('test.lib')
    lib = store['test.lib']
    lib.write('df_bench_random', df_random[idx])

    lib.write('series_bench_random', s_random[idx])

    lib.write('df_bench_compressible', df_compress[idx])

    lib.write('series_bench_compressible', s_compress[idx])

    lib.write('test_df', df_random[idx])

    lib.append('test_df', df_random[idx])

    lib.append('df_bench_random', df_random[idx])

    lib.append('series_bench_random', s_random[idx])

    lib.append('df_bench_compressible', df_compress[idx])

    lib.append('series_bench_compressible', s_compress[idx])


@pytest.mark.parametrize('fw_pointers_cfg', [FwPointersCfg.DISABLED, FwPointersCfg.HYBRID, FwPointersCfg.ENABLED])
def test_benchmark_writes(benchmark, fw_pointers_cfg):
    with FwPointersCtx(fw_pointers_cfg):
        benchmark.pedantic(write_data, iterations=5, rounds=10)


@pytest.mark.parametrize('fw_pointers_cfg', [FwPointersCfg.DISABLED, FwPointersCfg.HYBRID, FwPointersCfg.ENABLED])
def test_benchmark_reads(benchmark, fw_pointers_cfg):
    with FwPointersCtx(fw_pointers_cfg):
        benchmark.pedantic(read_data, iterations=5, rounds=10)


@pytest.mark.parametrize('fw_pointers_cfg', [FwPointersCfg.DISABLED, FwPointersCfg.HYBRID, FwPointersCfg.ENABLED])
def test_benchmark_append(benchmark, fw_pointers_cfg):
    with FwPointersCtx(fw_pointers_cfg):
        benchmark.pedantic(append_data, iterations=5, rounds=10)
